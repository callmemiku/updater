use std::ops::{Add, Deref};
use std::time::SystemTime;
use futures::future::join_all;
use futures::executor::ThreadPool;
use actix_web::web::{Json};
use actix_web::{HttpResponse};
use actix_web::dev::Service;
use actix_web::error::Error;
use chrono::Local;
use deadpool_postgres::{Config, ManagerConfig, Object, RecyclingMethod, Runtime};
use deadpool_postgres::tokio_postgres::{ NoTls};
use futures::{Future};
use futures_cpupool::CpuPool;
use crate::errors::MyError;
use crate::models::UpdateDTO;

macro_rules! either {
    ($test:expr => $true_expr:expr; $false_expr:expr) => {
        if $test {
            $true_expr
        }
        else {
            $false_expr
        }
    }
}

pub const APPLICATION_JSON: &str = "application/json";

#[get("/test")]
pub async fn test() -> Result<HttpResponse, Error> {
    println!("{}", Local::now().format("%Y-%m-%d %H:%M:%S"));
    Ok(HttpResponse::Ok().json("200"))
}

#[post("/batches")]
pub async fn batches(dto: Json<UpdateDTO>) -> Result<HttpResponse, MyError> {
    let cpus = num_cpus::get() / 2;
    let backup: String = format!("back_up_table_{}_rust_app_do_not_touch", &dto.table).to_string();
    let mut cfg = Config::new();
    cfg.host = Some(dto.host.to_string());
    cfg.user = Some(dto.usr.to_string());
    cfg.password = Some(dto.pwd.to_string());
    cfg.port = Some(5432);
    cfg.application_name = Some("RUST TUT".to_string());
    cfg.dbname = Some(dto.db.to_string());
    cfg.manager = Some(ManagerConfig { recycling_method: RecyclingMethod::Fast });
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    let client = pool.get().await.unwrap();
    let rows = client
        .query("select column_name from information_schema.columns where table_name = $1 and is_nullable = 'NO'", &[&dto.table])
        .await.unwrap();
    let key: String = rows[0].get(0);
    createBackUp(&client, &backup, &dto, &key).await.unwrap();
    println!("pre-works done!");
    let idsq = format!("select {akey} from {tab} order by {akey}", akey = key, tab = &dto.schema.to_string().add(".").add(&dto.table));
    let rows = client.query(&idsq, &[]).await.unwrap();
    let threshold = rows.len() / cpus;
    let start_notice = format!("select * from {schema}.{table} limit 1. --start: {time}", schema = &dto.schema, table = &dto.table, time = Local::now().format("%Y-%m-%d %H:%M:%S"));
    client.execute(&start_notice, &[]).await.unwrap();
    for i in 0..cpus {
        let min: i32 = rows[i * threshold].get(0);
        let max_index = either!(i == cpus - 1 => rows.len() - 1;(i + 1) * threshold);
        let max: i32 = rows[max_index].get(0);
        let query = batch(min, max, &dto, &key).await;
        println!("{}", query);
        tokio::task::spawn(call_parallel(cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap().get().await.unwrap(), query));
    }
    let end_notice = format!("select * from {schema}.{table} limit 1. --end: {time}", schema = &dto.schema, table = &dto.table, time = Local::now().format("%Y-%m-%d %H:%M:%S"));
    client.execute(&end_notice, &[]).await.unwrap();
    Ok(HttpResponse::Ok().json("200"))
}

async fn batch(min: i32, max: i32, dto: &UpdateDTO, key: &String) -> String {
    let predicate = "AND ".to_string().add(&dto.predicate);
    format!("update {schema}.{table} set {field} = '{val}', modify_dttm = now()
                        WHERE {pkey} between {minid} and {maxid} {constr}",
        schema = &dto.schema,
        table = &dto.table,
        minid = min,
        maxid = max,
        pkey = key,
        constr = either!(dto.predicate.is_empty() => ""; &*predicate),
        field = &dto.field,
        val = &dto.value
    )
}

#[post("/update")]
pub async fn update(dto: Json<UpdateDTO>) -> Result<HttpResponse, MyError> {
    service(dto).await?;
    Ok(HttpResponse::Ok().json("200"))
}

pub async fn service(dto: Json<UpdateDTO>) -> Result<HttpResponse, MyError> {
    let cpus = num_cpus::get() / 2;
    let backup: String = format!("back_up_table_{}_rust_app_do_not_touch", &dto.table).to_string();
    let mut cfg = Config::new();
    println!("{} {} {} {}", dto.host.to_string(), dto.usr.to_string(), dto.pwd.to_string(), dto.db.to_string());
    cfg.host = Some(dto.host.to_string());
    cfg.user = Some(dto.usr.to_string());
    cfg.password = Some(dto.pwd.to_string());
    cfg.port = Some(5432);
    cfg.application_name = Some("RUST TUT".to_string());
    cfg.dbname = Some(dto.db.to_string());
    cfg.manager = Some(ManagerConfig { recycling_method: RecyclingMethod::Fast });
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    let client = pool.get().await.unwrap();
    let rows = client
        .query("select column_name from information_schema.columns where table_name = $1 and is_nullable = 'NO'", &[&dto.table])
        .await.unwrap();
    let key: String = rows[0].get(0);
    //createBackUp(&client, &backup, &dto, &key).await.unwrap();
    println!("after backup!");
    let idsq = format!("select {akey} from {tab} order by {akey}", akey = key, tab = &dto.schema.to_string().add(".").add(&dto.table));
    for i in 1..cpus + 1 {
        let query = buildMainQuery(&dto, &key, &backup, cpus.to_string(), i.to_string());
        tokio::task::spawn(call_parallel(cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap().get().await.unwrap(), query));
        //call(&client, &query).await.unwrap();
    }
    Ok(HttpResponse::Ok().json("200"))
}

async fn call(client: Object, query: &String) -> Result<HttpResponse, MyError> {
    client.execute(query, &[]).await.unwrap();
    Ok(HttpResponse::Ok().json("200"))
}

async fn call_parallel(client: Object, query: String) {
    client.execute(&query, &[]).await.unwrap();
    println!("done {time} !!!!", time = Local::now().format("%Y-%m-%d %H:%M:%S"))
}

fn buildMainQuery(dto: &UpdateDTO, pkey: &String, bup: &String, cpus: String, curr: String) -> String {
    let predicate = "AND ".to_string().add(&dto.predicate);
    format!("DO $do$
    DECLARE
        -- private variables (do not edit):
        total_time_start timestamp default clock_timestamp();
        total_time_elapsed numeric default 0; -- время выполнения всех запросов, в секундах
        query_time_start timestamp;
        query_time_elapsed numeric default 0; -- фактическое время выполнения 1-го запроса, в секундах
        estimated_time interval default null; -- оценочное время, сколько осталось работать
        rec_start record;
        rec_stop record;
        cycles int default 0; -- счётчик для цикла
        batch_rows int default 1; -- по сколько записей будем обновлять за 1 цикл
        processed_rows int default 0; -- счётчик, сколько записей обновили, увеличивается на каждой итерации цикла
        total_rows int default 0; -- количество записей всего

        -- public variables (need to edit):
        -- в этом запросе нужно исправить только название временной таблицы, остальное не трогать!
        cur CURSOR FOR SELECT * FROM {schema}.{backup_table} ORDER BY {key}; -- здесь д.б. именно временная таблица, сортировка по id обязательна!
        time_max constant numeric default 1; -- пороговое максимальное время выполнения 1-го запроса, в секундах (рекомендуется 1 секунда)
        cpu_num constant smallint default 1;
        cpu_max constant smallint default 1;
    BEGIN
        RAISE NOTICE 'Calculate total rows%', ' ';

        -- в этом запросе нужно исправить только название временной таблицы, остальное не трогать!
        SELECT COUNT(*) INTO total_rows FROM {schema}.{backup_table};

        PERFORM dblink_connect('host={host} port={port} dbname={db} user={usr} password={pwd}');

        FOR rec_start IN cur LOOP
                cycles := cycles + 1;

                FETCH RELATIVE (batch_rows - 1) FROM cur INTO rec_stop;

                IF rec_stop IS NULL THEN
                    batch_rows := total_rows - processed_rows;
                    FETCH LAST FROM cur INTO rec_stop;
                END IF;

                query_time_start := clock_timestamp();

                PERFORM dblink_exec('
                        update {schema}.{table} n
                        set {field} = '' {val} '', modify_dttm = now() FROM {schema}.{backup_table} as t
                        WHERE n.{key} % ' || {cpu_max} || ' = (' || {cpu_num} || ' - 1) AND t.{key} = n.{key} AND t.{key} BETWEEN ' || rec_start.{key} || ' AND ' || rec_stop.{key} {constr});
                        query_time_elapsed := round(extract('epoch' from clock_timestamp() - query_time_start)::numeric, 2);
                        total_time_elapsed := round(extract('epoch' from clock_timestamp() - total_time_start)::numeric, 2);
                        processed_rows := processed_rows + batch_rows;

                        IF cycles > 16 THEN
                            estimated_time := ((total_rows * total_time_elapsed / processed_rows - total_time_elapsed)::int::text || 's')::interval;
                        END IF;

                        RAISE NOTICE 'Query % processed % rows (id %% % = (% - 1) AND id BETWEEN % AND %) for % sec', cycles, batch_rows, {cpu_max}, {cpu_num}, rec_start, rec_stop, query_time_elapsed;
                        RAISE NOTICE 'Total processed % of % rows (% %%)', processed_rows, total_rows, round(processed_rows * 100.0 / total_rows, 2);
                        RAISE NOTICE 'Current date time: %, elapsed time: %, estimated time: %', clock_timestamp()::timestamp(0), (clock_timestamp() - total_time_start)::interval(0), COALESCE(estimated_time::text, '?');
                        RAISE NOTICE '%', ' '; -- just new line

                        IF query_time_elapsed < time_max THEN
                            batch_rows := batch_rows * 2;
                        ELSE
                            batch_rows := GREATEST(1, batch_rows / 2);
                        END IF;
                    END LOOP;

                PERFORM dblink_disconnect();

                RAISE NOTICE 'Done. % rows per second, % queries per second', (processed_rows / total_time_elapsed)::int, round(cycles / total_time_elapsed, 2);

            END
    $do$ language plpgsql;",
            schema = &dto.schema,
            field = &dto.field,
            table = &dto.table,
            backup_table = bup,
            key = pkey,
            val = &dto.value,
            cpu_max = cpus,
            cpu_num = curr,
            constr = either!(dto.predicate.is_empty() => ""; &*predicate),
            host = &dto.host,
            db = &dto.db,
            port = either!(dto.port.is_empty() => "5432"; &dto.port),
            usr = &dto.usr,
            pwd = &dto.pwd)
}

async fn createBackUp(client: &Object, backup: &String, dto: &UpdateDTO, pkey: &String) -> Result<HttpResponse, MyError> {

    let val: String = backup.to_string().add("_unique_id");
    let table = &dto.schema.to_string().add(".").add(&dto.table);
    let backup = &dto.schema.to_string().add(".").add(backup);

    let vacuum = format!("vacuum verbose analyse {tab}", tab = table);
    client.query(&vacuum, &[]).await.unwrap();

    /*let drop = format!("DROP TABLE IF EXISTS {}", backup);
    client.query(&drop, &[]).await.unwrap();

    let create = format!("create table if not exists {} as (select * from {})", backup, table);
    client.query(&create, &[]).await.unwrap();

    let index = format!("CREATE UNIQUE INDEX {} ON {} ({})", val, backup, pkey);
    client.query(&index, &[]).await.unwrap();*/

    Ok(HttpResponse::Ok().json("ok"))
}