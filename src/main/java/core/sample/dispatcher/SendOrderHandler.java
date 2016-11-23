package com.scotiaitrade.oms.dispatcher;

import java.sql.Connection;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scotiaitrade.oms.app.OMSAppBase;
import com.scotiaitrade.oms.clients.ClientStateIntf;
import com.scotiaitrade.oms.common.Constants;
import com.scotiaitrade.oms.common.EventType;
import com.scotiaitrade.oms.common.InstrumentType;
import com.scotiaitrade.oms.common.OrderState;
import com.scotiaitrade.oms.common.OrderStatus;
import com.scotiaitrade.oms.common.TransactionType;
import com.scotiaitrade.oms.data.COOrder;
import com.scotiaitrade.oms.data.EventInfo;
import com.scotiaitrade.oms.data.OrderBase;
import com.scotiaitrade.oms.data.OrderCountCache;
import com.scotiaitrade.oms.db.DatabaseInterface;
import com.scotiaitrade.oms.db.PoolTypesEnum;
import com.scotiaitrade.oms.exceptions.OMSDBException;
import com.scotiaitrade.oms.mq.OrderSenderIntf;

public abstract class SendOrderHandler implements Runnable {
    protected static Logger log = LoggerFactory.getLogger(SendOrderHandler.class);
    protected OrderBase order;
    protected DatabaseInterface dbIntf;
    protected OrderSenderIntf orderSender;
    protected ClientStateIntf clientStateEngine;

    protected SendOrderHandler(DatabaseInterface dbIntf, OrderSenderIntf orderSender, OrderBase order, ClientStateIntf clientStateEngine) {
        this.order = order;
        this.dbIntf = dbIntf;
        this.orderSender = orderSender;
        this.clientStateEngine = clientStateEngine;
    }

    public abstract void run();

    protected boolean processSent() {
        Connection conn = null;
        boolean needsRollback = false;
        try {
            if (order.getInstrumentType() == InstrumentType.CO) {
            	COOrder coOrder = (COOrder) order;
            	if (coOrder.isResendForQuery()) {
            		log.info("Resending order for CEP query succeeded: " + coOrder.getOrderID());
            		return !needsRollback;
            	}
            	// handle CO, order changes directly to PENDING/CLOSED states after being sent
            	OrderStatus status = OrderStatus.UNDEFINED;
                if (coOrder.getTransactionType() == TransactionType.NEW) {
                    status = OrderStatus.SENT_NEW;
                } else if (coOrder.getTransactionType() == TransactionType.CORR) {
                	status = OrderStatus.SENT;
                } else if (coOrder.getTransactionType() == TransactionType.CXL) {
                	status = OrderStatus.CANCELLED;
                } else {
                    log.error("Unknown transaction type");
                }
                if (status != OrderStatus.UNDEFINED) {
                    coOrder.setStatus(status);
                    coOrder.setOrderBookState(status.getOrderState());
                    coOrder.setCOState(status.getCOState());
                    clientStateEngine.setClientState(coOrder);
                    coOrder.setLegStatus(status, status.getOrderState(), clientStateEngine);
                }
                
                // lock the order
                conn = dbIntf.getConnection(PoolTypesEnum.ORDER_SUBMISSION, false);
                Date eventTime = new Date();
                int eventNum = dbIntf.getNextEventNo(order.getRootOrderID(), conn);
                EventInfo eventInfo = new EventInfo(order, EventType.SENT, eventNum, eventTime);
                // no need to call setCounterIncrements as SENT and SENDING correspond to the same counter
                eventInfo.setSourceIDs(Constants.OMS, Constants.OMS);
                
                // get the order state that is in the database
                OrderState previousState = OrderState.SENDING;
                OrderBase previousOrder = OMSAppBase.getOrder(dbIntf, order.getRootOrderID(), order.getOrderID(), order.getInstrumentType(), conn, log);
                if (previousOrder != null) {
                    previousState = previousOrder.getOrderBookState();
                } else {
                    log.warn("Failed to check order state before updating SENT state");
                }
                
                // update SENT state and status only if the order state is as expected
                if (previousState == OrderState.SENDING) {
                    order.callDBUpdateOrderStatus(dbIntf, conn);
                } else { // out-of-sequence order status
                    log.warn("Detected out-of-sequence order status before SENT status: " + previousState.name());
                }
                
                // unlock the order
                dbIntf.addEventInfo(eventInfo, conn);
                if ( !dbIntf.commitTransaction(conn) ) {
                    log.error("Failed to commit db transaction");
                    needsRollback = true;
                }
            } else { // handle all other instrument types
                order.setOrderBookState(OrderState.SENT);
                if (order.getStatus() == OrderStatus.SENDING_NEW)
                	order.setStatus(OrderStatus.SENT_NEW);
                else
                	order.setStatus(OrderStatus.SENT);
                
                // lock the order
                conn = dbIntf.getConnection(PoolTypesEnum.ORDER_SUBMISSION, false);
                int eventNum = dbIntf.getNextEventNo(order.getRootOrderID(), conn);
                EventInfo eventInfo = new EventInfo(order, EventType.SENT, eventNum, new Date());
                // no need to call setCounterIncrements as SENT and SENDING correspond to the same counter
                eventInfo.setSourceIDs(Constants.OMS, Constants.OMS);
                
                // get the order state that is in the database
                OrderState previousState = OrderState.SENDING;
                OrderBase previousOrder = OMSAppBase.getOrder(dbIntf, order.getRootOrderID(), order.getOrderID(), order.getInstrumentType(), conn, log);
                if (previousOrder != null) {
                    previousState = previousOrder.getOrderBookState();
                } else {
                    log.warn("Failed to check order state before updating SENT state");
                }
                
                // update SENT state and status only if the order state is as expected
                if (previousState == OrderState.SENDING) {
                	if (order.getRootContingencyID() != null && order.getContingencyID() != null) {
                		COOrder coOrder = (COOrder) OMSAppBase.getOrder(dbIntf, order.getRootContingencyID(),
                				order.getContingencyID(), InstrumentType.CO, conn, log);
                		coOrder.updateOrderFromLeg(order, dbIntf, conn);
                	}
                    order.callDBUpdateOrderStatus(dbIntf, conn);
                } else { // out-of-sequence order status
                    log.warn("Detected out-of-sequence order status before SENT status: " + previousState.name());
                }
                
                // unlock the order
                dbIntf.addEventInfo(eventInfo, conn);
                if ( !dbIntf.commitTransaction(conn) ) {
                    log.error("Failed to commit db transaction");
                    needsRollback = true;
                }
            }
        } catch (OMSDBException ex) {
            log.error("Failed to update sent state for: " + order.getOrderID(), ex);
            needsRollback = true;
        } catch (Exception ex) {
            log.error("Failed to sent state: " + order.getOrderID(), ex);
            needsRollback = true;
        }
        if ( needsRollback && conn != null ) {
            if ( !dbIntf.rollbackTransaction(conn)) {
                log.error("Failed to rollback db transaction");
            }
        }
        if ( conn != null ) {
            dbIntf.close(conn);
        }

        return !needsRollback;
    }
    
    public boolean processUnSent() {
        Connection conn = null;
        boolean needsRollback = false;
        try {
            if (order.getInstrumentType() == InstrumentType.CO) {
                COOrder coOrder = (COOrder) order;
                if (coOrder.isResendForQuery()) {
            		log.warn("Resending order for CEP query failed: " + coOrder.getOrderID());
            		return !needsRollback;
            	}
                coOrder.setPrevStatus(coOrder.getStatus());
                coOrder.setPrevOrderBookState(coOrder.getOrderBookState());
                coOrder.setPrevCOState(coOrder.getCOState());
                OrderStatus status = OrderStatus.UNDEFINED;
                if (coOrder.getTransactionType() == TransactionType.NEW){
                	status = OrderStatus.UNSENT_NEW;
                } else if (coOrder.getTransactionType() == TransactionType.CORR) {
                	status = OrderStatus.UNSENT;
                } else if (coOrder.getTransactionType() == TransactionType.CXL) {
                	status = OrderStatus.CANCELLED;
                } else {
                    log.error("Unknown transaction type");
                }
                if (status != OrderStatus.UNDEFINED) {
                    coOrder.setStatus(status);
                    coOrder.setOrderBookState(status.getOrderState());
                    coOrder.setCOState(status.getCOState());
                    clientStateEngine.setClientState(coOrder);
                    coOrder.setLegStatus(status, status.getOrderState(), clientStateEngine);
                }
                
                // lock the order
                conn = dbIntf.getConnection(PoolTypesEnum.ORDER_SUBMISSION, false);
                Date eventTime = new Date();
                int eventNum = dbIntf.getNextEventNo(order.getRootOrderID(), conn);
                EventInfo eventInfo = new EventInfo(order, EventType.UNSENT, eventNum, eventTime);
                eventInfo.setSourceIDs(Constants.OMS, Constants.OMS);
                OrderCountCache.setCountIncrements(order);
                
                order.callDBUpdateOrderStatus(dbIntf, conn);
                dbIntf.addEventInfo(eventInfo, conn);
                
                // unlock the order
                if ( !dbIntf.commitTransaction(conn) ) {
                    log.error("Failed to commit db transaction");
                    needsRollback = true;
                }
            } else { // handle all other instrument types
                order.setPrevStatus(order.getStatus());
                order.setPrevOrderBookState(order.getOrderBookState());
                order.setOrderBookState(OrderState.UNSENT);
                if (order.getStatus() == OrderStatus.SENDING_NEW)
                    order.setStatus(OrderStatus.UNSENT_NEW);
                else
                    order.setStatus(OrderStatus.UNSENT);
                clientStateEngine.setClientState(order);
                
                // lock the order
                conn = dbIntf.getConnection(PoolTypesEnum.ORDER_SUBMISSION, false);
                int eventNum = dbIntf.getNextEventNo(order.getRootOrderID(), conn);
                EventInfo eventInfo = new EventInfo(order, EventType.UNSENT, eventNum, new Date());
                eventInfo.setSourceIDs(Constants.OMS, Constants.OMS);
                OrderCountCache.setCountIncrements(order);
                
                if (order.getRootContingencyID() != null && order.getContingencyID() != null) {
            		COOrder coOrder = (COOrder) OMSAppBase.getOrder(dbIntf, order.getRootContingencyID(),
            				order.getContingencyID(), InstrumentType.CO, conn, log);
            		coOrder.updateOrderFromLeg(order, dbIntf, conn);
            	}
                order.callDBUpdateOrderStatus(dbIntf, conn);
                dbIntf.addEventInfo(eventInfo, conn);
                // unlock the order
                if ( !dbIntf.commitTransaction(conn) ) {
                    log.error("Failed to commit db transaction");
                    needsRollback = true;
                }
            }
        } catch (OMSDBException ex) {
            log.error("Failed to update unsent state for: " + order.getOrderID(), ex);
            needsRollback = true;
        } catch (Exception ex) {
            log.error("Failed to update unsent state for: " + order.getOrderID(), ex);
            needsRollback = true;
        }
        if ( needsRollback && conn != null ) {
            if ( !dbIntf.rollbackTransaction(conn)) {
                log.error("Failed to rollback db transaction");
            }
        }
        if ( conn != null ) {
            dbIntf.close(conn);
        }

        return !needsRollback;
    }
}

