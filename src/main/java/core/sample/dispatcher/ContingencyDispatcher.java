package com.scotiaitrade.oms.dispatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.TaskExecutor;

import com.scotiaitrade.oms.clients.ClientStateIntf;
import com.scotiaitrade.oms.data.CEPMarketStructure;
import com.scotiaitrade.oms.data.Gateway;
import com.scotiaitrade.oms.data.OrderBase;
import com.scotiaitrade.oms.db.DatabaseInterface;
import com.scotiaitrade.oms.mq.OrderSenderIntf;

public class ContingencyDispatcher extends OrderDispatcher {
	protected Logger log = LoggerFactory.getLogger(ContingencyDispatcher.class);
		
    public ContingencyDispatcher(DatabaseInterface dbIntf, 
    		OrderSenderIntf contingencySender, TaskExecutor taskExecutor) {
    	super(dbIntf, contingencySender, taskExecutor);
    }
    
    public boolean sendFailedOrder(OrderBase order, Gateway gateway, 
    		ClientStateIntf clientStateEngine, boolean async, boolean checkStatus) {
    	SendFailedOrderHandler handler = new SendFailedOrderHandler(
                dbIntf, orderSender, order, clientStateEngine);
    	return sendOrder(order, handler, gateway, async, checkStatus);
    }
    
    public boolean sendMarketStructureGroup(CEPMarketStructure cepMarketStructure, 
    		Gateway gateway, boolean async, boolean checkStatus) {
    	SendMarketStructureGroupHandler handler = new SendMarketStructureGroupHandler(
                dbIntf, orderSender, cepMarketStructure);
    	return sendMarketStructure(handler, gateway, async, checkStatus);
    }
        
    protected boolean sendMarketStructure(SendMarketStructureGroupHandler handler,
    		Gateway gateway, boolean async, boolean checkStatus) {
    	boolean retVal = true;
    	if (!gateway.isRouteEnabled()) {
    		log.warn("Gateway is not route enabled: " + gateway);
            retVal = false;
    	} else if (!gateway.isMonitored()) {
    		log.warn("Gateway is not monitored: " + gateway);
    		if (async) {
            	retVal = dispatch(handler);
            } else {
            	handler.run();
            }
    	} else if (checkStatus) {
        	switch (gateway.getStatus()) {
            case UP:
            	if (async) {
                	retVal = dispatch(handler);
                } else {
                	handler.run();
                }
                break;
            case DOWN_UNEXPECTED:
            case DISCONNECTED_ORDER_Q:
            case DISCONNECTED_REPORT_Q:
            	log.warn("Gateway is down. " + gateway.getGatewayName() + " (market structure)");
                retVal = false;
                break;
            case DOWN_EXPECTED:
            	log.warn("Gateway is out of service hours - market structure will not be sent: " + gateway.getGatewayName());
                retVal = false;
                break;
            }
        } else {
        	if (async) {
            	retVal = dispatch(handler);
            } else {
            	handler.run();
            }
        }
        if (!retVal) {
        	handler.processUnSent();
        }
        return retVal;
    }
}
