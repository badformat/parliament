package io.github.parliament.kv;

import java.util.concurrent.Future;

import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespData;

/**
 *
 * @author zy
 */
interface KeyValueEngine {
    Future<RespData> execute(RespArray request) throws Exception;
}