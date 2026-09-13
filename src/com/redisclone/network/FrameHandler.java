package com.redisclone.network;

import com.redisclone.resp.RespFrame;

/**
 * Functional callback interface for handling decoded RESP frames and client disconnections.
 */
public interface FrameHandler {
    void handleFrame(ClientConnection client, RespFrame frame);
    void onClientDisconnected(ClientConnection client);
}
