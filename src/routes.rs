use actix_web::{web, HttpResponse, Scope};

use crate::{
    common::{Command, KeyName},
    error::Result,
    handler::handle,
    state::GlobalState,
};

macro_rules! handle_fn {
    (
        $(
            ($fname:ident, $cmd:expr);
        )+
    ) => {
        $(
            async fn $fname(
                body: web::Bytes,
                key_name: web::Path<KeyName>,
                global_store: web::Data<GlobalState>,
            ) -> Result<HttpResponse> {
                handle(body, key_name, global_store, $cmd).await
            }
        )+
    };
}

handle_fn! {
    (handle_lpush, Command::LPUSH);
    (handle_rpush, Command::RPUSH);
    (handle_lpop, Command::LPOP);
    (handle_rpop, Command::RPOP);
    (handle_randpop, Command::RANDPOP);
    (handle_lrange, Command::LRANGE);
    (handle_rrange, Command::RRANGE);
    (handle_lindex, Command::LINDEX);
    (handle_lrand, Command::LRAND);
    (handle_llen, Command::LLEN);
    (handle_ldel, Command::LDEL);
    (handle_lrm, Command::LRM);
    (handle_hget, Command::HGET);
    (handle_hset, Command::HSET);
    (handle_hsetnx, Command::HSETNX);
    (handle_hgetall, Command::HGETALL);
    (handle_hmget, Command::HMGET);
    (handle_hmset, Command::HMSET);
    (handle_hincrby, Command::HINCRBY);
    (handle_hkeys, Command::HKEYS);
    (handle_hvals, Command::HVALS);
    (handle_hexists, Command::HEXISTS);
    (handle_hdel, Command::HDEL);
    (handle_hlen, Command::HLEN);
    (handle_hrm, Command::HRM);
    (handle_alpush, Command::ALPUSH);
    (handle_alpushnx, Command::ALPUSHNX);
    (handle_arpush, Command::ARPUSH);
    (handle_arpushnx, Command::ARPUSHNX);
    (handle_aincrby, Command::AINCRBY);
    (handle_alpop, Command::ALPOP);
    (handle_arpop, Command::ARPOP);
    (handle_arandpop, Command::ARANDPOP);
    (handle_aget, Command::AGET);
    (handle_arand, Command::ARAND);
    (handle_alrange, Command::ALRANGE);
    (handle_arrange, Command::ARRANGE);
    (handle_akeys, Command::AKEYS);
    (handle_avals, Command::AVALS);
    (handle_aall, Command::AALL);
    (handle_aexists, Command::AEXISTS);
    (handle_alen, Command::ALEN);
    (handle_adel, Command::ADEL);
    (handle_arm, Command::ARM);
}

pub fn make_route() -> Scope {
    web::scope("/")
        .route("/lpush/{key}", web::post().to(handle_lpush))
        .route("/rpush/{key}", web::post().to(handle_rpush))
        .route("/lpop/{key}", web::post().to(handle_lpop))
        .route("/rpop/{key}", web::post().to(handle_rpop))
        .route("/randpop/{key}", web::post().to(handle_randpop))
        .route("/lrange/{key}", web::post().to(handle_lrange))
        .route("/rrange/{key}", web::post().to(handle_rrange))
        .route("/lindex/{key}", web::post().to(handle_lindex))
        .route("/lrand/{key}", web::post().to(handle_lrand))
        .route("/llen/{key}", web::post().to(handle_llen))
        .route("/ldel/{key}", web::post().to(handle_ldel))
        .route("/lrm/{key}", web::post().to(handle_lrm))
        .route("/hget/{key}", web::post().to(handle_hget))
        .route("/hset/{key}", web::post().to(handle_hset))
        .route("/hsetnx/{key}", web::post().to(handle_hsetnx))
        .route("/hgetall/{key}", web::post().to(handle_hgetall))
        .route("/hmget/{key}", web::post().to(handle_hmget))
        .route("/hmset/{key}", web::post().to(handle_hmset))
        .route("/hincrby/{key}", web::post().to(handle_hincrby))
        .route("/hkeys/{key}", web::post().to(handle_hkeys))
        .route("/hvals/{key}", web::post().to(handle_hvals))
        .route("/hexists/{key}", web::post().to(handle_hexists))
        .route("/hdel/{key}", web::post().to(handle_hdel))
        .route("/hlen/{key}", web::post().to(handle_hlen))
        .route("/hrm/{key}", web::post().to(handle_hrm))
        .route("/alpush/{key}", web::post().to(handle_alpush))
        .route("/alpushnx/{key}", web::post().to(handle_alpushnx))
        .route("/arpush/{key}", web::post().to(handle_arpush))
        .route("/arpushnx/{key}", web::post().to(handle_arpushnx))
        .route("/aincrby/{key}", web::post().to(handle_aincrby))
        .route("/alpop/{key}", web::post().to(handle_alpop))
        .route("/arpop/{key}", web::post().to(handle_arpop))
        .route("/arandpop/{key}", web::post().to(handle_arandpop))
        .route("/aget/{key}", web::post().to(handle_aget))
        .route("/arand/{key}", web::post().to(handle_arand))
        .route("/alrange/{key}", web::post().to(handle_alrange))
        .route("/arrange/{key}", web::post().to(handle_arrange))
        .route("/akeys/{key}", web::post().to(handle_akeys))
        .route("/avals/{key}", web::post().to(handle_avals))
        .route("/aall/{key}", web::post().to(handle_aall))
        .route("/aexists/{key}", web::post().to(handle_aexists))
        .route("/alen/{key}", web::post().to(handle_alen))
        .route("/adel/{key}", web::post().to(handle_adel))
        .route("/arm/{key}", web::post().to(handle_arm))
}
