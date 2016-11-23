package com.scotiaitrade.oms.dispatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scotiaitrade.oms.clients.ClientStateIntf;
import com.scotiaitrade.oms.data.COOrder;
import com.scotiaitrade.oms.data.EQOrder;
import com.scotiaitrade.oms.data.OPOrder;
import com.scotiaitrade.oms.data.OrderBase;
import com.scotiaitrade.oms.db.DatabaseInterface;
import com.scotiaitrade.oms.mq.OrderSenderIntf;

public class SendCorrectionOrderHandler extends SendOrderHandler {
    private static Logger log = LoggerFactory.getLogger(SendCorrectionOrderHandler.class);

    public SendCorrectionOrderHandler(DatabaseInterface dbIntf, OrderSenderIntf orderSender, OrderBase order, ClientStateIntf clientStateEgine) {
        super(dbIntf, orderSender, order, clientStateEgine);
        log = LoggerFactory.getLogger(SendCorrectionOrderHandler.class);
    }

    public void run() {
        boolean sent = false;
        switch ( order.getInstrumentType() ) {
          case EQ:
            sent = orderSender.sendCorrectionOrder((EQOrder)order);
            break;
          case EOP:
          case IOP:
            sent = orderSender.sendCorrectionOrder((OPOrder)order);
            break;
          case CO:
        	sent = orderSender.sendCorrectionOrder((COOrder) order);
        	break;
          default:
            log.error("Can't send order unsupported InstrumentType: " + order.getInstrumentType() );
            return;
        }
        if ( sent ) {
        	log.info("Sent CFO order: " + order.getOrderID());
    		if ( !super.processSent() ) {
    			log.error("Failed to updated order status after new order was sent.");
    		}
        } else {
            log.error("Failed to send CFO order: " + order.getOrderID());
            if ( !super.processUnSent() ) {
                log.error("Failed to update order status after failed send.");
            }
        }
    }
}

