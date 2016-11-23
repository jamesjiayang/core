package com.scotiaitrade.oms.dispatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scotiaitrade.oms.data.CEPMarketStructure;
import com.scotiaitrade.oms.db.DatabaseInterface;
import com.scotiaitrade.oms.mq.OrderSenderIntf;

public class SendMarketStructureGroupHandler implements Runnable {
	protected static Logger log = LoggerFactory.getLogger(SendMarketStructureGroupHandler.class);
	
	protected OrderSenderIntf orderSender;
	protected CEPMarketStructure cepMarketStructure;
	
	public SendMarketStructureGroupHandler(DatabaseInterface dbIntf, OrderSenderIntf orderSender, CEPMarketStructure cepMarketStructure) {
        log = LoggerFactory.getLogger(SendNewOrderHandler.class);
        this.orderSender = orderSender;
        this.cepMarketStructure = cepMarketStructure;
    }

    public void run() {
        boolean sent = orderSender.sendMarketStructureGroups(cepMarketStructure);
        
        if ( sent ) {
            log.info("Market structure group is sent.");
        } else {
            log.error("Failed to send market structure group.");
        }
    }

	public void processUnSent() {
		log.info("Market structure group is unsent.");
	}
}
