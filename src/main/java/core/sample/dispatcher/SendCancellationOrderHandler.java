package com.scotiaitrade.oms.dispatcher;

import org.slf4j.Logger;
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

public class SendCancellationOrderHandler extends SendOrderHandler {
    private static Logger log = LoggerFactory.getLogger(SendCorrectionOrderHandler.class);

    public SendCancellationOrderHandler(DatabaseInterface dbIntf, OrderSenderIntf orderSender, OrderBase order, ClientStateIntf clientStateEngine) {
        super(dbIntf, orderSender, order, clientStateEngine);
        log = LoggerFactory.getLogger(SendCancellationOrderHandler.class);
    }

    public void run() {
        boolean sent = false;
        switch ( order.getInstrumentType() ) {
          case EQ:
            sent = orderSender.sendCancellationOrder((EQOrder)order);
            break;
          case EOP:
          case IOP:
            sent = orderSender.sendCancellationOrder((OPOrder)order);
            break;
          case MF:
        	  sent = orderSender.sendCancellationOrder((MFOrder)order);
              break;
          case FI:
              sent = orderSender.sendCancellationOrder((FIOrder)order);
              break;
          case CO:
              sent = orderSender.sendCancellationOrder((COOrder)order);
              break;
          default:
            log.error("Can't send order unsupported InstrumentType: " + order.getInstrumentType() );
            return;
        }
        if ( sent ) {
        	log.info("Sent CXL order: " + order.getOrderID());
            if ( !super.processSent() ) {
                log.error("Failed to update order status after CXL order was sent.");
            }
        } else {
            log.error("Failed to send CXL order: " + order.getOrderID());
            if ( !super.processUnSent() ) {
                log.error("Failed to update order status after failed send.");
            }
        }
    }
}

