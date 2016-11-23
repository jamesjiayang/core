package com.scotiaitrade.oms.dispatcher;

import org.slf4j.LoggerFactory;

import com.scotiaitrade.oms.clients.ClientStateIntf;
import com.scotiaitrade.oms.data.COOrder;
import com.scotiaitrade.oms.data.OrderBase;
import com.scotiaitrade.oms.db.DatabaseInterface;
import com.scotiaitrade.oms.mq.OrderSenderIntf;

public class SendFailedOrderHandler extends SendOrderHandler {

    public SendFailedOrderHandler(DatabaseInterface dbIntf, OrderSenderIntf orderSender, OrderBase order, ClientStateIntf clientStateEgine) {
        super(dbIntf, orderSender, order, clientStateEgine);
        log = LoggerFactory.getLogger(SendNewOrderHandler.class);
    }

    public void run() {
        boolean sent = false;
        switch ( order.getInstrumentType() ) {
        	case CO:
        		sent = orderSender.sendFailedOrder((COOrder)order);
        		break;
          default:
            log.error("Can't send order unsupported InstrumentType: " + order.getInstrumentType() );
            return;
        }
        if ( sent ) {
    	    log.info("Failed order was sent: " + order.getOrderID());
        } else {
            log.error("Failed to send failed order.");
        }
    }
}
