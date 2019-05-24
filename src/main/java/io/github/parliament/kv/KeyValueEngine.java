package io.github.parliament.kv;

import java.util.concurrent.Future;

import io.github.parliament.resp.RespArray;
import io.github.parliament.resp.RespData;

/**
 *
 * @author zy
 */
interface KeyValueEngine {
    void start() throws Exception;

    Future<RespData> execute(RespArray request) throws Exception;
}