use std::ops::Add;
use std::time::Duration;
use actix_web::web::{Json, Path};
use serde_derive::{Serialize, Deserialize};
use tokio_postgres::{Client, Config, NoTls};
use crate::{errors::MyError};
use actix_web::{Error, HttpResponse, HttpResponseBuilder};

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
#[derive(Debug, Deserialize, Serialize)]
pub struct UpdateDTO {
    pub url: String,
    pub schema: String,
    pub table: String,
    pub field: String,
    pub value: String,
    pub predicate: String,
    pub host: String,
    pub db: String,
    pub port: String,
    pub usr: String,
    pub pwd: String,
}

/*impl UpdateDTO {
    pub fn new(message: Json<UpdateDTO>) -> Self {
        Self {
            url: v["url"].to_string(),
            schema: v["schema"].to_string(),
            table: v["table"].to_string(),
            field: v["field"].to_string(),
            value: v["value"].to_string(),
            predicate: v["predicate"].to_string(),
            host: v["url"].to_string(),
            db: v["db"].to_string(),
            port: v["port"].to_string(),
            usr: v["usr"].to_string(),
            pwd: v["pwd"].to_string()
        }
    }
}*/
#[get("/test")]
pub async fn test() -> Result<HttpResponse, MyError> {
    let config = format!("host={host} user={user} password={pw} dbname={db}", host = "localhost", db = "test", user = "postgres", pw = "postgres");
    println!("{}", &config);
    let (client, _connection) = Config::new()
        .dbname("test")
        .host("localhost")
        .user("postgres")
        .password("postgres")
        .connect_timeout(Duration::new(5, 0))
        .connect(NoTls)
        .await
        .unwrap();
    println!("client da!");
    let r = client.execute("SELECT * from parallel.core_person_address limit 5", &[]).await?;
    println!("{}", r.to_string());
    Ok(HttpResponse::Ok().json("200"))
}

#[post("/update")]
pub async fn update(dto: Json<UpdateDTO>) -> Result<HttpResponse, MyError> {
    service(dto).await?;
    Ok(HttpResponse::Ok()
        .json("200"))
}

pub async fn service(dto: Json<UpdateDTO>) -> Result<HttpResponse, MyError> {
    let cpus = num_cpus::get() / 2;
    let backup: String = format!("back_up_table_{}_rust_app_do_not_touch", &dto.table).to_string();
    let config = format!("host={host} user={user} password={pw} dbname={db}", host = &dto.host, db = &dto.db, user = &dto.usr, pw = &dto.pwd);
    let (client, connection) = tokio_postgres::connect(&*config, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("conn err: {}", e)
        }
    });
    let keyQuery = client.prepare("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = $1 and IS_NULLABLE = 'NO';").await.unwrap();
    let rows = client
        .query(&keyQuery, &[&dto.table])
        .await?;
    let key = rows[0].get(0);
    createBackUp(&client, &backup, &dto.table, &key).await?;
    for i in 1..cpus + 1 {
        let str = buildMainQuery(&dto, &key, &backup, cpus, i);
        call(&client, &str).await?;
    }
    Ok(HttpResponse::Ok().json("200"))
}

async fn call(client: &Client, query: &String) -> Result<HttpResponse, MyError> {
    client.query(query, &[]).await?;
    Ok(HttpResponse::Ok().json("200"))
}

fn buildMainQuery(dto: &UpdateDTO, pkey: &String, bup: &String, cpus: usize, curr: usize) -> String {
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
                cur CURSOR FOR SELECT * FROM {backup_table} ORDER BY {key}; -- здесь д.б. именно временная таблица, сортировка по id обязательна!
                time_max constant numeric default 1; -- пороговое максимальное время выполнения 1-го запроса, в секундах (рекомендуется 1 секунда)
                cpu_num constant smallint default 1;
                cpu_max constant smallint default 1;
       BEGIN
           RAISE NOTICE 'Calculate total rows%', ' ';

                -- в этом запросе нужно исправить только название временной таблицы, остальное не трогать!
                SELECT COUNT(*) INTO total_rows FROM {backup_table};

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
                               set {field} = {val} as value, modify_dttm = now()
                        FROM {schema}.{backup_table} as t
                        WHERE n.{key} % ' || '{cpu_max} || ' = (' || {cpu_num} || ' - 1)
                        AND t.{key} = n.{key} AND t.{key} BETWEEN ' ||
                                            rec_start.{key} || ' AND ' || rec_stop.{key}
                            {constr}
                            );

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

async fn createBackUp(client: &Client, bup: &String, table: &String, pkey: &String) -> Result<HttpResponse, MyError> {
    let val: String = "".to_string().add(bup).add("_unique_id");
    client.query("VACUUM VERBOSE ANALYZE $1;", &[table]).await?;
    client.query("DROP TABLE IF EXISTS $1;", &[bup]).await?;
    client.query("create table if not exists $1 as (select * from $2);", &[bup, table]).await?;
    client.query("CREATE UNIQUE INDEX $1 ON $2 ($3);", &[&val, bup, pkey]).await?;
    Ok(HttpResponse::Ok().json("ok"))
}