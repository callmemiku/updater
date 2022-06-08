use std::collections::HashMap;
use std::ops::Add;
use actix_web::HttpResponse;
use actix_web::error::Error;
use actix_web::web::{Json};
use chrono::{Local};
use deadpool_postgres::{Config, ManagerConfig, Object, RecyclingMethod, Runtime};
use deadpool_postgres::tokio_postgres::NoTls;
use substring::Substring;

use crate::errors::MyError;
use crate::models::{UpdateDTO, UpdateFromViewDTO};

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

const BATCH_SIZE: i64 = 5000;

#[get("/test")]
pub async fn test() -> Result<HttpResponse, Error> {
    println!("{}", Local::now().format("%Y-%m-%d %H:%M:%S"));
    Ok(HttpResponse::Ok().json("200"))
}

#[deprecated(since="1.3.0", note="please use `batches_selective` instead")]
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
    println!("Found primary key - {k}", k = key);
    let map = dto_to_map(&dto).await;
    createBackUp(&client, &backup, map, &key).await.unwrap();
    println!("pre-works done!");
    let idsq = format!("select {akey}::bigint from {tab} order by {akey}::bigint ",
                       akey = key,
                       tab = &dto.schema.to_string().add(".").add(&dto.table));
    let rows = client.query(&idsq, &[]).await.unwrap();
    println!("Found {k} rows!", k = rows.len());
    let threshold = rows.len() / cpus;
    let start_notice = format!("select * from {schema}.{table} limit 1. --start: {time}", schema = &dto.schema, table = &dto.table, time = Local::now().format("%Y-%m-%d %H:%M:%S"));
    client.execute(&start_notice, &[]).await.unwrap();
    for i in 0..cpus {
        let min: i64 = rows[i * threshold].get(0);
        let max_index = either!(i == cpus - 1 => rows.len() - 1;(i + 1) * threshold);
        let max: i64 = rows[max_index].get(0);
        let query = batch(min, max, &dto, &key).await;
        println!("{}", query);
        tokio::task::spawn(call_parallel(cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap().get().await.unwrap(), query, ));
    }
    let end_notice = format!("select * from {schema}.{table} limit 1. --end: {time}", schema = &dto.schema, table = &dto.table, time = Local::now().format("%Y-%m-%d %H:%M:%S"));
    client.execute(&end_notice, &[]).await.unwrap();
    Ok(HttpResponse::Ok().json("200"))
}

async fn batch(min: i64, max: i64, dto: &UpdateDTO, key: &String) -> String {
    let predicate = "AND ".to_string().add(&dto.predicate);
    format!("update {schema}.{table} set {field} = {val}, modify_dttm = now()
                        WHERE {pkey}::numeric between {minid} and {maxid} {constr}",
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
    let map = dto_to_map(&dto).await;
    createBackUp(&client, &backup, map, &key).await.unwrap();
    println!("after backup!");
    for i in 1..cpus + 1 {
        let query = buildMainQuery(&dto, &key, &backup, cpus.to_string(), i.to_string());
        tokio::task::spawn(call_parallel(cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap().get().await.unwrap(), query));
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

async fn dto_to_map(dto: &UpdateDTO) -> HashMap<&str, String> {
    let backup: String = format!("back_up_table_{}_rust_app_do_not_touch", &dto.table).to_string();
    let mut map: HashMap<&str, String> = HashMap::new();
    map.insert("schema", dto.schema.clone());
    map.insert("field", dto.field.clone());
    map.insert("table", dto.table.clone());
    map.insert("backup", backup);
    map.insert("table", dto.table.clone());
    map.insert("predicate", dto.predicate.clone());
    return map;
}

async fn vdto_to_map(dto: &UpdateFromViewDTO) -> HashMap<&str, String> {
    let backup: String = format!("back_up_table_{}_rust_app_do_not_touch", &dto.table).to_string();
    let mut map: HashMap<&str, String> = HashMap::new();
    map.insert("schema", dto.schema.clone());
    map.insert("field", dto.field.clone());
    map.insert("table", dto.table.clone());
    map.insert("backup", backup);
    map.insert("table", dto.table.clone());
    map.insert("predicate", ("").parse().unwrap());
    return map;
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
        cur CURSOR FOR SELECT * FROM {schema}.{backup_table} ORDER BY {key}::numeric; -- здесь д.б. именно временная таблица, сортировка по id обязательна!
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
                        update {schema}.{table} as n
                        set {field} = ''{val}'', modify_dttm = now() FROM {schema}.{backup_table} as t
                        WHERE n.{key}::numeric % ' || {cpu_max} || ' = (' || {cpu_num} || ' - 1) AND t.{key}::numeric = n.{key}::numeric AND t.{key}::numeric BETWEEN ' || rec_start.{key}::numeric || ' AND ' || rec_stop.{key}::numeric {constr});
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

async fn createBackUp(client: &Object, backup: &String, dto: HashMap<&str, String>, pkey: &String) -> Result<HttpResponse, MyError> {
    let constraint = "WHERE ".to_string().add(&dto["predicate"]);
    let val: String = backup.to_string().add("_unique_id");
    let table = dto["schema"].clone().add(".").add(&*dto["table"]);
    let backup = dto["schema"].clone().add(".").add(backup);
    let vacuum = format!("vacuum verbose analyse {tab}", tab = table);
    client.query(&vacuum, &[]).await.unwrap();
    println!("Vacuumed.");
    let drop = format!("DROP TABLE IF EXISTS {}", backup);
    client.query(&drop, &[]).await.unwrap();
    println!("Dropped backup if exist.");
    let create = format!("create table if not exists {} as (select {}::bigint, {} from {} {con})", backup, pkey, dto["field"], table,
                         con = either!(dto["predicate"].is_empty() => ""; &*constraint));
    client.query(&create, &[]).await.unwrap();
    println!("Created backup.");
    let index = format!("CREATE INDEX {} ON {} (({}::numeric))", val, backup, pkey);
    client.query(&index, &[]).await.unwrap();
    println!("Index added.");
    Ok(HttpResponse::Ok().json("ok"))
}

#[deprecated(since="1.5.0", note="please use `batches_selective` instead")]
//#[post("/batches-selective")]
pub async fn batches_selective_uneffective(dto: Json<UpdateDTO>) -> Result<HttpResponse, MyError> {
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
    println!("Found primary key - {k}", k = key);
    //createBackUp(&client, &backup, &dto, &key).await.unwrap();
    println!("pre-works done!");
    let predicate = &*"WHERE ".to_string().add(&dto.predicate);
    let pkey_sql = format!("select {pkey}::bigint from {schema}.{tab} {con} order by {pkey}::bigint",
                           tab = &dto.table,
                           pkey = &key,
                           schema = &dto.schema,
                           con = either!(dto.predicate.is_empty() => ""; predicate));
    let ids = client.query(&pkey_sql, &[]).await.unwrap();
    println!("Found {} records!", ids.len());
    let start = Local::now();
    let start_notice = format!("select * from {schema}.{table} limit 1. --start: {time}", schema = &dto.schema, table = &dto.table, time = start.format("%Y-%m-%d %H:%M:%S"));
    client.execute(&start_notice, &[]).await.unwrap();
    let threshold = ids.len() / cpus;
    println!("Splitting rows between threads...");
    for i in 0..cpus {
        let min: i64 = (i * threshold) as i64;
        let max: i64 = either!((i + 1) * threshold > ids.len() => ids.len(); (i + 1) * threshold) as i64;
        let mut local: Vec<i64> = vec![];
        for j in min..max {
            let val: i64 = ids[j as usize].get(0);
            local.push(val);
        }
        let mut map = HashMap::new();
        map.insert("schema", dto.schema.clone());
        map.insert("table", dto.table.clone());
        map.insert("field", dto.field.clone());
        map.insert("value", dto.value.clone());
        map.insert("key", key.clone());
        map.insert("total", cpus.clone().to_string());
        map.insert("number", (i + 1).to_string());
        map.insert("predicate", dto.predicate.clone());
        let client = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap().get().await.unwrap();
        println!("\tSending {} / {} done.", i + 1, cpus);
        //thread_pool.handle().spawn_blocking(move || {call_parallel_batch(client, ids, task_num,i, cpus, map)});
        tokio::task::spawn(call_parallel_batch(client,  local,map));
    }
    println!("Total time elapsed: {} seconds.", Local::now().signed_duration_since(start).to_std().unwrap().as_secs());
    let end_notice = format!("select * from {schema}.{table} limit 1. --end: {time}", schema = &dto.schema, table = &dto.table, time = Local::now().format("%Y-%m-%d %H:%M:%S"));
    client.execute(&end_notice, &[]).await.unwrap();
    Ok(HttpResponse::Ok().json("Sent to DB successfully!"))
}

#[post("/batches-selective")]
pub async fn batches_selective(dto: Json<UpdateDTO>) -> Result<HttpResponse, MyError> {
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
    println!("Found primary key - {k}", k = key);
    let map = dto_to_map(&dto).await;
    createBackUp(&client, &backup, map, &key).await.unwrap();
    println!("Pre-update works done!");
    println!("Gathering needed IDs...");
    let pkey_sql = format!("select {pkey} from {schema}.{tab} order by {pkey}",
                           tab = backup,
                           pkey = &key,
                           schema = &dto.schema);
    let ids = client.query(&pkey_sql, &[]).await.unwrap();
    println!("Found {} records!", ids.len());
    let start = Local::now();
    let start_notice = format!("select * from {schema}.{table} limit 1. --start: {time}", schema = &dto.schema, table = &dto.table, time = start.format("%Y-%m-%d %H:%M:%S"));
    client.execute(&start_notice, &[]).await.unwrap();
    let threshold = (ids.len() / cpus) + 1;
    println!("Splitting rows between threads...");
    for i in 0..cpus {
        let min: i64 = (i * threshold) as i64;
        let max: i64 = either!((i + 1) * threshold > ids.len() => ids.len(); (i + 1) * threshold) as i64;
        let mut local: Vec<i64> = vec![];
        for j in min..max {
            let val: i64 = ids[j as usize].get(0);
            local.push(val);
        }
        let mut map = HashMap::new();
        map.insert("schema", dto.schema.clone());
        map.insert("table", dto.table.clone());
        map.insert("field", dto.field.clone());
        map.insert("value", dto.value.clone());
        map.insert("key", key.clone());
        map.insert("total", cpus.clone().to_string());
        map.insert("number", (i + 1).to_string());
        map.insert("predicate", dto.predicate.clone());
        let client = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap().get().await.unwrap();
        println!("\tSending {} / {} done.", i + 1, cpus);
        tokio::task::spawn(call_parallel_batch(client,local, map));
    }
    println!("Total time elapsed: {} seconds.", Local::now().signed_duration_since(start).to_std().unwrap().as_secs());
    let end_notice = format!("select * from {schema}.{table} limit 1. --end: {time}", schema = &dto.schema, table = &dto.table, time = Local::now().format("%Y-%m-%d %H:%M:%S"));
    client.execute(&end_notice, &[]).await.unwrap();
    Ok(HttpResponse::Ok().json("Sent to DB successfully!"))
}

async fn call_parallel_batch(client: Object, ids: Vec<i64>, map: HashMap<&str, String>) {
    let local_total = ids.len() as i64;
    let start = Local::now();
    let times = (local_total / BATCH_SIZE) + 1;
    for i in 0..times {
        println!("\t\tTask#{}: Preparing batch {} / {}.", map["number"], i + 1, times);
        let mut local_ids: Vec<i64> = vec![];
        let min = i * BATCH_SIZE;
        let temp = ((i + 1) * BATCH_SIZE) as i64;
        let max = either!(temp > local_total => local_total; temp);
        for j in min..max {
            let val: i64 = ids[j as usize];
            local_ids.push(val);
        }
        let ids_as_string = "(".to_string().add(&local_ids.iter().map(|val| val.to_string() + "), (").collect::<String>());
        let str_id = ids_as_string.substring(0, ids_as_string.len() - 3)
            .parse()
            .unwrap();
        let query = batch_in(str_id, &map).await;
        println!("\t\tTask#{}: Sending batch {} / {}.", map["number"], i + 1, times);
        client.query(&query, &[]).await.unwrap();
        println!("\t\tTask#{}: Batches sent {} / {}.", map["number"], i + 1, times);
    }
    let dur = Local::now().signed_duration_since(start).to_std().unwrap().as_secs();
    println!("\t\tTask#{num} - Done {num} / {tot}, time elapsed: {time} seconds!", tot = map["total"], num = map["number"], time = dur);

}

async fn batch_in(ids: String, map: &HashMap<&str, String>) -> String {
    format!("update {schema}.{table} set {field} = {val}, modify_dttm = now()
                        WHERE {pkey}::numeric = ANY(VALUES {id})",
            schema = map["schema"],
            table = map["table"],
            id = ids,
            pkey = map["key"],
            field = map["field"],
            val = map["value"]
    )
}

#[post("/from-view")]
pub async fn update_from_view(dto: Json<UpdateFromViewDTO>) -> Result<HttpResponse, MyError> {
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
    println!("Found primary key - {k}", k = key);
    let map = vdto_to_map(&dto).await;
    createBackUp(&client, &backup, map, &key).await.unwrap();
    for i in 0..cpus {
        let query = update_view_query(&dto, i, cpus, &key).await;
        println!("{}", query);
        tokio::task::spawn(call_parallel(cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap().get().await.unwrap(), query));
    }
    Ok(HttpResponse::Ok().json("ok"))
}

async fn update_view_query(dto: &UpdateFromViewDTO, curr: usize, total: usize, key: &String) -> String {
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
        cur CURSOR FOR SELECT * FROM {schema}.{view} ORDER BY object_record_id; -- здесь д.б. именно временная таблица, сортировка по id обязательна!
        time_max constant numeric default 1; -- пороговое максимальное время выполнения 1-го запроса, в секундах (рекомендуется 1 секунда)
        cpu_num constant smallint default 1;
        cpu_max constant smallint default 1;
        query text;
    BEGIN
        RAISE NOTICE 'Calculate total rows%', ' ';

        -- в этом запросе нужно исправить только название временной таблицы, остальное не трогать!
        SELECT COUNT(*) INTO total_rows FROM {schema}.{view};

        PERFORM dblink_connect('host={host} port={port} dbname={db} user={usr} password={pw}');

        SELECT 'UPDATE {schema}.{table} n
                         SET {field}
                             = t.person_idfl_new, modify_dttm = now()
                         FROM   {schema}.{view} t
                         WHERE  t.object_record_id = n.{pkey}'
        FROM   information_schema.columns
        WHERE  table_name = '{view}'
          AND  table_schema = '{schema}'
          AND  column_name = 'person_idfl_new' into query;

        FOR rec_start IN cur LOOP
                cycles := cycles + 1;

                FETCH RELATIVE (batch_rows - 1) FROM cur INTO rec_stop;

                IF rec_stop IS NULL THEN
                    batch_rows := total_rows - processed_rows;
                    FETCH LAST FROM cur INTO rec_stop;
                END IF;

                query_time_start := clock_timestamp();

                PERFORM dblink_exec(query ||
                                        ' AND n.{pkey} % ' || {cpus} || ' = (' || {cpu} || ' - 1)
                                          AND t.object_record_id = n.{pkey} AND t.object_record_id BETWEEN ' ||
                                    rec_start.object_record_id || ' AND ' || rec_stop.object_record_id || ' and t.person_idfl_new <> null'
                    );

                query_time_elapsed := round(extract('epoch' from clock_timestamp() - query_time_start)::numeric, 2);
                total_time_elapsed := round(extract('epoch' from clock_timestamp() - total_time_start)::numeric, 2);
                processed_rows := processed_rows + batch_rows;

                IF cycles > 16 THEN
                    estimated_time := ((total_rows * total_time_elapsed / processed_rows - total_time_elapsed)::int::text || 's')::interval;
                END IF;

                RAISE NOTICE 'Query % processed % rows (id %% % = (% - 1) AND id BETWEEN % AND %) for % sec', cycles, batch_rows, {cpus}, {cpu}, rec_start, rec_stop, query_time_elapsed;
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
$do$ language plpgsql",
        schema = &dto.schema,
        cpu = curr,
        cpus = total,
        field = &dto.field,
        view = &dto.view,
        table = &dto.table,
        host = &dto.host,
        port = either!(dto.port.is_empty() => "5432"; &dto.port),
        db = &dto.db,
        usr = &dto.usr,
        pw = &dto.pwd,
        pkey = key,
    )
}

#[post("/view-batches")]
pub async fn view_batches(dto: Json<UpdateFromViewDTO>) -> Result<HttpResponse, MyError> {
    let mut cpus = num_cpus::get() / 2;
    let backup: String = format!("back_up_table_{}_rust_app_do_not_touch", &dto.table).to_string();
    let iter_table = format!("iter_table_for_{}_do_not_touch", &dto.view);
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
    println!("Found primary key - {k}", k = key);
    let map = vdto_to_map(&dto).await;
    println!("Preparing back-up...");
    //createBackUp(&client, &backup, map, &key).await.unwrap();
    println!("Preparing iter-table...");
    client.query(&format!("drop table if exists {}.{}", &dto.schema, iter_table), &[]).await.unwrap();
    let iter_q = format!("create table {schema}.{iter} as (select object_record_id::numeric as {k}, {f} from {schema}.{view})", schema = &dto.schema, iter = iter_table, k = key, f = &dto.view_field, view = &dto.view);
    client.query(&iter_q, &[]).await.unwrap();
    client.query(&format!("CREATE INDEX {} ON {}.{} (({}::numeric))", "index_iterate".to_string().add(&*iter_table), &dto.schema, iter_table, key), &[]).await.unwrap();
    println!("Iter-table created!");
    println!("Pre-update works done!");
    println!("Gathering needed IDs...");
    let pkey_sql = format!("select {pkey}::bigint from {schema}.{tab} order by {pkey}",
                           tab = iter_table,
                           pkey = &key,
                           schema = &dto.schema);
    let ids = client.query(&pkey_sql, &[]).await.unwrap();
    println!("Found {} records!", ids.len());
    let start = Local::now();
    let start_notice = format!("select * from {schema}.{table} limit 1. --start: {time}", schema = &dto.schema, table = &dto.table, time = start.format("%Y-%m-%d %H:%M:%S"));
    client.execute(&start_notice, &[]).await.unwrap();
    let threshold = either!((ids.len() / cpus) + 1 < (BATCH_SIZE as usize)  => ids.len(); (ids.len() / cpus) + 1);
    println!("Splitting rows between threads...");
    cpus = either!(threshold == ids.len() => 1; num_cpus::get() / 2);
    for i in 0..cpus {
        let min: i64 = (i * threshold) as i64;
        let max: i64 = either!((i + 1) * threshold > ids.len() => ids.len(); (i + 1) * threshold) as i64;
        let mut local: Vec<i64> = vec![];
        for j in min..max {
            let val: i64 = ids[j as usize].get(0);
            local.push(val);
        }
        let mut map = HashMap::new();
        map.insert("schema", dto.schema.clone());
        map.insert("table", dto.table.clone());
        map.insert("field", dto.field.clone());
        map.insert("key", key.clone());
        map.insert("total", cpus.clone().to_string());
        map.insert("number", (i + 1).to_string());
        map.insert("view", dto.view.clone());
        map.insert("iter_table", iter_table.clone());
        map.insert("view_field", dto.view_field.clone());
        let client = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap().get().await.unwrap();
        println!("\tSending {} / {} done.", i + 1, cpus);
        tokio::task::spawn(call_parallel_batch_view(client,local, map));
    }
    println!("Total time elapsed: {} seconds.", Local::now().signed_duration_since(start).to_std().unwrap().as_secs());
    let end_notice = format!("select * from {schema}.{table} limit 1. --end: {time}", schema = &dto.schema, table = &dto.table, time = Local::now().format("%Y-%m-%d %H:%M:%S"));
    client.execute(&end_notice, &[]).await.unwrap();
    //let iter_q = format!("drop table {}.{}", &dto.schema, iter_table);
    //client.query(&iter_q, &[]).await.unwrap();
    Ok(HttpResponse::Ok().json("Sent to DB successfully!"))
}

async fn call_parallel_batch_view(client: Object, ids: Vec<i64>, map: HashMap<&str, String>) {
    let local_total = ids.len() as i64;
    let start = Local::now();
    let times = (local_total / BATCH_SIZE) + 1;
    for i in 0..times {
        println!("\t\tTask#{}: Preparing batch {} / {}.", map["number"], i + 1, times);
        let mut local_ids: Vec<i64> = vec![];
        let min = i * BATCH_SIZE;
        let temp = ((i + 1) * BATCH_SIZE) as i64;
        let max = either!(temp > local_total => local_total; temp);
        for j in min..max {
            let val: i64 = ids[j as usize];
            local_ids.push(val);
        }
        let ids_as_string = "(".to_string().add(&local_ids.iter().map(|val| val.to_string() + "), (").collect::<String>());
        let str_id = ids_as_string.substring(0, ids_as_string.len() - 3)
            .parse()
            .unwrap();
        let query = batch_view(str_id, &map).await;

        println!("{}", query);

        println!("\t\tTask#{}: Sending batch {} / {}.", map["number"], i + 1, times);
        client.query(&query, &[]).await.unwrap();
        println!("\t\tTask#{}: Batches sent {} / {}.", map["number"], i + 1, times);
    }
    let dur = Local::now().signed_duration_since(start).to_std().unwrap().as_secs();
    println!("\t\tTask#{num} - Done {num} / {tot}, time elapsed: {time} seconds!", tot = map["total"], num = map["number"], time = dur);

}

async fn batch_view(ids: String, map: &HashMap<&str, String>) -> String {
    format!("update {schema}.{table} set {field} = {schema}.{iter}.{view_field}, modify_dttm = now() from {schema}.{iter}
                        WHERE {schema}.{iter}.{pkey} = ANY(VALUES {id})
                        and {schema}.{table}.{pkey} = {schema}.{iter}.{pkey}",
            schema = map["schema"],
            table = map["table"],
            id = ids,
            pkey = map["key"],
            field = map["field"],
            iter = map["iter_table"],
            view_field = map["view_field"]
    )
}