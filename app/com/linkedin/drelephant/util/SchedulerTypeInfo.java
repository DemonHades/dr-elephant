package com.linkedin.drelephant.util;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Created by biyan on 16/12/7.
 */

@XmlRootElement(name = "scheduler")
@XmlAccessorType(XmlAccessType.FIELD)
public class SchedulerTypeInfo {
    protected SchedulerInfo schedulerInfo;

    public SchedulerTypeInfo() {
    } // JAXB needs this

    public SchedulerTypeInfo(final SchedulerInfo scheduler) {
        this.schedulerInfo = scheduler;
    }

    public SchedulerInfo getSchedulerInfo() {
        return schedulerInfo;
    }

}