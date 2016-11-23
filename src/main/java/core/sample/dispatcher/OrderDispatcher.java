package com.scotiaitrade.oms.dispatcher;

import java.util.AbstractQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.TaskExecutor;

import com.scotiaitrade.oms.clients.ClientStateIntf;
import com.scotiaitrade.oms.data.Gateway;
import com.scotiaitrade.oms.data.OrderBase;
import com.scotiaitrade.oms.db.DatabaseInterface;
import com.scotiaitrade.oms.mq.OrderSenderIntf;

public class OrderDispatcher {
    private Logger log = LoggerFactory.getLogger(OrderDispatcher.class);

    protected TaskExecutor taskExecutor;
    protected OrderSenderIntf orderSender;
    protected DatabaseInterface dbIntf;

    public OrderDispatcher(DatabaseInterface dbIntf, OrderSenderIntf theOrderSender, TaskExecutor taskExecutor) {
        this.dbIntf = dbIntf;
        orderSender = theOrderSender;
        this.taskExecutor = taskExecutor;
    }
    
    public boolean sendNewOrder(OrderBase order, Gateway gateway, 
    		ClientStateIntf clientStateEngine, boolean async, boolean checkStatus) {
        SendNewOrderHandler handler = new SendNewOrderHandler(dbIntf,
                                              orderSender, order, clientStateEngine);
        return sendOrder(order, handler, gateway, async, checkStatus);
    }

    public boolean sendCorrectionOrder(OrderBase order, Gateway gateway, 
    		ClientStateIntf clientStateEngine, boolean async, boolean checkStatus) {
        SendCorrectionOrderHandler handler = new SendCorrectionOrderHandler(
                                                  dbIntf, orderSender, order, clientStateEngine);
        return sendOrder(order, handler, gateway, async, checkStatus);
    }

    public boolean sendCancellationOrder(OrderBase order, Gateway gateway, 
    		ClientStateIntf clientStateEngine, boolean async, boolean checkStatus) {
        SendCancellationOrderHandler handler = new SendCancellationOrderHandler(
                                                  dbIntf, orderSender, order, clientStateEngine);
        return sendOrder(order, handler, gateway, async, checkStatus);
    }

    public boolean releaseOrderQueue(Gateway gateway, ClientStateIntf clientStateEngine) {
        return dispatch(new ReleaseOrderQueue(gateway, clientStateEngine));
    }

    public void clearOrderQueue(final Gateway gateway) {
        dispatch(new Runnable() {
            public void run() {
                gateway.clearOrderQueue();
            }
        });
    }

    protected boolean sendOrder(OrderBase order, SendOrderHandler handler,
                                Gateway gateway, boolean async, boolean checkStatus) {
        boolean retVal = true;
        order.setCompID(gateway.getCompID());
        order.setSubID(gateway.getSubID());
        if (!gateway.isRouteEnabled()) {
    		log.warn("Gateway is not route enabled: " + order.getGateway() + " " + order.getOrderID());
            retVal = false;
    	} else if (!gateway.isMonitored()) {
    		log.warn("Gateway is not monitored: " + order.getGateway() + " " + order.getOrderID());
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
                //gateway.queueOrder(order);
                log.warn("Gateway is down. " + order.getGateway() + " " + order.getOrderID());
                retVal = false;
                break;
            case DOWN_EXPECTED:
                log.warn("Gateway is out of service hours - order will not be sent: " + order.getGateway() + " " + order.getOrderID());
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

    protected boolean dispatch(Runnable task) {
        try {
            taskExecutor.execute(task);
        } catch (Exception ex) {
            log.error("Failed to dispatch task.", ex);
            return false;
        }
        return true;
    }

    class ReleaseOrderQueue implements Runnable {
        Gateway gateway;
        ClientStateIntf clientStateEngine;

        public ReleaseOrderQueue(Gateway gateway, ClientStateIntf clientStateEngine) {
            this.gateway = gateway;
            this.clientStateEngine = clientStateEngine;
        }

        public void run() {
            AbstractQueue<OrderBase> orderQueue = gateway.getOrderQueue();
            OrderBase order = orderQueue.poll();
            SendOrderHandler handler = null;
            while ( order != null ) {
                switch ( order.getTransactionType() ) {
                  case NEW:
                    handler = new SendNewOrderHandler(
                                          dbIntf, orderSender, order, clientStateEngine);
                    sendOrder(order, handler, gateway, true, true);
                    break;
                  case CORR:
                    handler = new SendCorrectionOrderHandler(
                                               dbIntf, orderSender, order, clientStateEngine);
                    sendOrder(order, handler, gateway, true, true);
                    break;
                  case CXL:
                    handler = new SendCancellationOrderHandler(
                                               dbIntf, orderSender, order, clientStateEngine);
                    sendOrder(order, handler, gateway, true, true);
                    break;
                  default:
                    log.error("TransactionType is unsupported: " + order.getTransactionType() + " for order: " + order.getOrderID());
                    break;
                }
            }
        }
    }


}
