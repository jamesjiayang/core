/**
 * 
 */
package com.scotiaitrade.oms.mq.pool;

import javax.jms.ConnectionFactory;

/**
 * An exception-unaware JMS connection factory
 * <p/>
 * Connection factories implement this interface are meant to be exception-unaware such that they do not 
 * (or more likely cannot) listen for exceptions on Connections they provide.
 * <p/>
 * A purge method is available to purge all pooled connections and it is up to the callers of this factory 
 * to explicitly call purge when needed (eg, upon a JMSException on a connection).  
 * 
 * @author blai
 *
 */
public interface ExceptionUnawareConnectionFactory extends ConnectionFactory {
	
	/**
	 * Purge all pooled connections
	 */
	void purge();
}
