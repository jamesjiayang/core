package com.scotiaitrade.oms.dispatcher;

import org.slf4j.LoggerFactory;

import com.scotiaitrade.oms.clients.ClientStateIntf;
import com.scotiaitrade.oms.data.COOrder;
import com.scotiaitrade.oms.data.EQOrder;
import com.scotiaitrade.oms.data.FIOrder;
import com.scotiaitrade.oms.data.MFOrder;
import com.scotiaitrade.oms.data.OPOrder;
import com.scotiaitrade.oms.data.OrderBase;
import com.scotiaitrade.oms.db.DatabaseInterface;
import com.scotiaitrade.oms.mq.OrderSenderIntf;

public class SendNewOrderHandler extends SendOrderHandler {

    public SendNewOrderHandler(DatabaseInterface dbIntf, OrderSenderIntf orderSender, OrderBase order, ClientStateIntf clientStateEgine) {
        super(dbIntf, orderSender, order, clientStateEgine);
        log = LoggerFactory.getLogger(SendNewOrderHandler.class);
    }

    public void run() {
        boolean sent = false;
        switch ( order.getInstrumentType() ) {
          case EQ:
            sent = orderSender.sendNewOrder((EQOrder)order);
            break;
          case EOP:
          case IOP:
            sent = orderSender.sendNewOrder((OPOrder)order);
            break;
          case MF:
              sent = orderSender.sendNewOrder((MFOrder)order);
              break;
          case FI:
              sent = orderSender.sendNewOrder((FIOrder)order);
              break;
          case NI:
              log.debug("No need to send order with InstrumentType: " + order.getInstrumentType() );
              return;
          case CO:
        	  sent = orderSender.sendNewOrder((COOrder)order);
        	  break;
          default:
            log.error("Can't send order unsupported InstrumentType: " + order.getInstrumentType() );
            return;
        }
        if ( sent ) {
        	log.info("Sent NEW order: " + order.getOrderID());
    		if ( !super.processSent() ) {
    			log.error("Failed to update order status after new order was sent.");
    		}
        } else {
            log.error("Failed to send NEW order: " + order.getOrderID());
            if ( !super.processUnSent() ) {
                log.error("Failed to update order status after failed send.");
            }
        }
    }
}

