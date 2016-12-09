package com.linkedin.drelephant.util;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerQueueInfo;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by biyan on 16/12/7.
 */
public class SchedulerQueueInfoLoader {
    private static final Logger logger = Logger.getLogger(SchedulerQueueInfoLoader.class);

    @SuppressWarnings("unchecked")
    private static <T> T converyToJavaBean(String xml, Class<T> c) {
        T t = null;
        try {
            JAXBContext context = JAXBContext.newInstance(c);
            Unmarshaller unmarshaller = context.createUnmarshaller();
            t = (T) unmarshaller.unmarshal(new StringReader(xml));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.print(e);
        }

        return t;
    }

    private static <T> T getXMLObject(Class<T> cls,String serverURL) {
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod(serverURL);
        method.addRequestHeader("Content-Type", "application/xml");
        method.addRequestHeader("Accept", "application/xml");
        try {
            int statusCode = client.executeMethod(method);
            if (statusCode != HttpStatus.SC_OK) {
                return null;
            }
            byte[] bytes = method.getResponseBody();
            String result = new String(bytes, "utf-8");
            return converyToJavaBean(result, cls);
        } catch (IOException e) {
            return null;
        } finally {
            method.releaseConnection();
        }
    }

    public static Map<String, FairSchedulerQueueInfo> loadSchedulerInfo() {
        Map<String, FairSchedulerQueueInfo> queueInfos = new HashMap<String, FairSchedulerQueueInfo>();
        String schedulerUrl = "http://rz-data-hdp-rm01.rz.sankuai.com:8088/ws/v1/cluster/scheduler";
        SchedulerTypeInfo schedulerTypeInfo = getXMLObject(SchedulerTypeInfo.class, schedulerUrl);
        if (schedulerTypeInfo == null){
            return null;
        }
        FairSchedulerInfo fairSchedulerInfo = (FairSchedulerInfo)schedulerTypeInfo.schedulerInfo;
        Collection<FairSchedulerQueueInfo> childQueues = fairSchedulerInfo.getRootQueueInfo().getChildQueues();
        for (FairSchedulerQueueInfo fairSchedulerQueueInfo : childQueues) {
            queueInfos.put(fairSchedulerQueueInfo.getQueueName(), fairSchedulerQueueInfo);
            if (fairSchedulerQueueInfo.getChildQueues() != null){
                for (FairSchedulerQueueInfo child : fairSchedulerQueueInfo.getChildQueues()) {
                    queueInfos.put(child.getQueueName(), child);
                }
            }
        }
        return queueInfos;
    }

}
