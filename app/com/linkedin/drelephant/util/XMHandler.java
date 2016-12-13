package com.linkedin.drelephant.util;

import com.alibaba.fastjson.JSONObject;
import com.sankuai.xm.pub.push.Pusher;
import com.sankuai.xm.pub.push.PusherBuilder;
import org.apache.log4j.Logger;

/**
 * Created by biyan on 16/11/24.
 */
public class XMHandler {

    private static final Logger logger = Logger.getLogger(XMHandler.class);

    private static short APPID = 1;
    private static String APPKEY = "MqyNwifg6EuVIOC8";
    private static String TOKEN = "ab0d09a9b271e373b24f99caed44bb0c";
    private static long PUBID = 310375;
    private static String PUBNAME = "离线计算托管平台";
    private static String P2P_URL = "http://dxw-in.sankuai.com/api/pub/push";
    private static String BROADCAST_URL = "http://dxw-in.sankuai.com/api/pub/pubBroadcast";

    public static Pusher pusher;

    static {
        pusher = PusherBuilder.defaultBuilder()
                .withAppkey(APPKEY)
                .withApptoken(TOKEN)
                .withTargetUrl(P2P_URL)
                .withFromName(PUBNAME)
                .withFromUid(PUBID)
                .build();
        logger.info("[Meituan] Pub init successfully!");
    }

    public static boolean sendMessage(String msg, String... receiver) {
        try {
            JSONObject result = pusher.push(msg, receiver);
        } catch (Exception e) {
            logger.error("[Meituan] Pub failed to send message!");
            return false;
        }
        return true;
    }
}
