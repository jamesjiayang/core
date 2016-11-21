/**
 * 
 */
package com.scotiaitrade.oms.mq.pool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolUtils;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A special purpose JMS Connection Factory
 * <p/>
 * This home-grown connection factory provides cached connections.  Each connection has a pre-opened 
 * session which has a cached map of destination-to-message-producer pairs.  All conections, sessions,
 * and message producers will remain open until a purge of the connection pool happens.  Sessions are all 
 * non-transacted and auto-acknowledged.  Attempts to create other types of sessions will result in 
 * UnsupportedOperationException. 
 * <p/>
 * This Connection Factory is exception-unaware and implements the interface ExceptionUnawareConnectionFactory.
 * As such, it is the responsibility of the caller to explicitly invoke the provided purge method upon
 * an exception on a Connection.  This behavior is by design since some JMS providers (such as Websphere MQ) 
 * does not support/allow registering an exception listener by application code when running inside an
 * application container.
 * <p/>
 * The creation and use of this class is purely motivated by performance only.  
 * Container-provided ConnectionFactory should be used directly otherwise.
 * <p/>
 *  
 * @author blai
 *
 */
public class ProducerCachedConnectionFactory implements ExceptionUnawareConnectionFactory {
	
	private Logger logger = LoggerFactory.getLogger(ProducerCachedConnectionFactory.class);

	private ConnectionFactory sourceConnectionFactory;

	private ConnectionProxyFactory proxyFactory;
	private GenericObjectPool connectionPool;
	private GenericObjectPool.Config connectionPoolConfig = new GenericObjectPool.Config();
	private int initPoolSize;
	private long connectionTimeoutMillisecond;

	
	public GenericObjectPool getConnectionPool() {
		return connectionPool;
	}

	/**
	 * Set the source connection factory from which this connection factory will obtain
	 * its connections.
	 */
	public void setSourceConnectionFactory(ConnectionFactory sourceConnectionFactory) {
		this.sourceConnectionFactory = sourceConnectionFactory;
	}

	/**
	 * Set the initial pool size for our connection pool.
	 */
	public void setInitPoolSize(int initPoolSize) {
		this.initPoolSize = initPoolSize;
	}

	/**
	 * Set the minimum number of connections to be kept in pool
	 */
	public void setMinPoolSize(int minPoolSize ) {
		connectionPoolConfig.minIdle = minPoolSize;
	}

	/**
	 * Set the hard maximum of number of connections  
	 */
	public void setMaxPoolSize(int maxPoolSize ) {
		connectionPoolConfig.maxActive = maxPoolSize;
		connectionPoolConfig.maxIdle = maxPoolSize;
	}

	/**
	 * Set the time limit in milliseconds for waiting for a new connection
	 * to be created before giving up
	 */
	public void setConnectionTimeoutMillisecond(long connectionTimeoutMillisecond) {
		this.connectionTimeoutMillisecond = connectionTimeoutMillisecond;
	}

	/**
	 * Initialize this connection factory
	 */
	public void initialize() {
		proxyFactory = new ConnectionProxyFactory(sourceConnectionFactory, connectionTimeoutMillisecond);
		initConnectionPool();
	}

	/**
	 * Shutdown this connection factory
	 */
	public void close() {
		clearConnectionPool();
		proxyFactory.close();
	}

	/**
	 * Purge the connection pool
	 */
	public synchronized void purge() {
		clearConnectionPool();
		proxyFactory.purge();
		initConnectionPool();
	}
	
	/**
	 * Obtain a (cached) connection from this factory
	 */
	@Override
	public Connection createConnection() throws JMSException {
		try {
			if ( logger.isDebugEnabled() ) {
				logger.debug(String.format("Enter creationConnection. Current pool stats: %d active, %d idle", connectionPool.getNumActive(), connectionPool.getNumIdle()));
			}
			return (Connection) connectionPool.borrowObject();
		} catch (Exception e) {
			logger.error("ProducerCachedConnectionFactory failed to obtain JMS Connection", e);
			throw new JMSException("Failed to create Connection", e.toString());
		} finally {
			if ( logger.isDebugEnabled() ) {
				logger.debug(String.format("creationConnection ended. Current pool stats: %d active, %d idle", connectionPool.getNumActive(), connectionPool.getNumIdle()));
			}
		}
	}

	@Override
	public Connection createConnection(String user, String pwd) throws JMSException {
		throw new UnsupportedOperationException("creationConnection call with authentication data is NOT supported by ProducerCachedConnectionFactory");
	}
	
	@Override
	protected void finalize() throws Throwable {
		close();
	}

	private void clearConnectionPool() {
		try {
			if ( connectionPool != null ) {
				connectionPool.clear();		// clear the connection pools objects. 
			}
		} catch (Exception e) {
			logger.warn("Failed to close connection pool", e);
		}
	}

	private void initConnectionPool() {
		if ( connectionPool == null ) {
			connectionPool = new GenericObjectPool();
			proxyFactory.setConnectionPool(connectionPool);
			connectionPool.setConfig(connectionPoolConfig);
			connectionPool.setFactory(proxyFactory);
		
			if ( initPoolSize > 0 ) {
				try {
					PoolUtils.prefill(connectionPool, initPoolSize);
				} catch (Exception e) {
					logger.warn("ProducerCachedConnectionFactory failed to pre-load connection pool", e);
				}
			}
		}
	}
	
	private static interface ProducerCachedConnection extends Connection {
		void closeTargetConnection();
	}
	
	/**
	 * An internal factory class for creating dynamic proxies for JMS connections
	 * to be put in a object pool
	 */
	private static class ConnectionProxyFactory extends BasePoolableObjectFactory {
		private Logger logger = LoggerFactory.getLogger(ConnectionProxyFactory.class);
		private static final String JMS_RESOURCE_CREATION_STOP = "stop";
		private static final String JMS_RESOURCE_CREATION_CONNECTION = "connection";
		
		private ConnectionFactory sourceConnectionFactory;
		private Destination destination;
		private ObjectPool connectionPool;
		private long connectionRequestTimeoutMillisecond;
		
		private BlockingQueue<String> connectionRequestQueue;
		private BlockingQueue<Object> connectionResponseQueue;
		private boolean isClosed;

		
		public ConnectionProxyFactory(
				ConnectionFactory sourceConnectionFactory,
				long connectionRequestTimeoutMillisecond) {
			super();
			this.sourceConnectionFactory = sourceConnectionFactory;
			this.connectionRequestTimeoutMillisecond = connectionRequestTimeoutMillisecond;
			
			connectionRequestQueue = new LinkedBlockingQueue<String>();
			connectionResponseQueue = new LinkedBlockingQueue<Object>();
			initAsyncWorker();
		}
		
		public void setConnectionPool(ObjectPool connectionPool) {
			this.connectionPool = connectionPool;
		}

		/**
		 * Close this factory, no more operations beyond this point
		 */
		public synchronized void close() {
			logger.info("ProducerCachedConnectionFactory.ConnectionProxyFactory is closing");
			purge();
			connectionRequestQueue.add(JMS_RESOURCE_CREATION_STOP);
			isClosed = true;
		}

		/**
		 * Purge any pending requests
		 * This is meant to be called upon a JMS exception on a resource produced by this factory. 
		 */
		public synchronized void purge() {
			if ( !isClosed ) {
				connectionRequestQueue.clear();
				connectionResponseQueue.clear();
				logger.info("Pending connection requests have been purged");
			}
		}
		
		@Override
		public void destroyObject(Object obj) throws Exception {
			try {
				((ProducerCachedConnection) obj).closeTargetConnection();
			} catch (Exception e) {
				// best effort close
			}
		}

		@Override
		public Object makeObject() throws Exception {
			connectionRequestQueue.add(JMS_RESOURCE_CREATION_CONNECTION);
			Object result = connectionResponseQueue.poll(connectionRequestTimeoutMillisecond, TimeUnit.MILLISECONDS);
			if ( result == null ) {
				throw new Exception("Timeout waiting for creation of JMS Connection");
			}
			if ( result instanceof Connection ) {
				return (Connection) result;
			}
			throw new Exception("Failed to create JMS Connection", (Throwable) result);
		}
		
		/**
		 * Set up a worker thread to manage resource creations.
		 * We have to use a ***NON-CONTAINER-MANAGED*** thread here for all JMS resource creation.  
		 * 
		 * In WAS, the container (expecting applications to use its ConnectionFactory 
		 * for JMS connections, as most applications would) will automatically
		 * close the JMS resources allocated from within a managed thread (eg. WorkManager) 
		 * when the thread is returned to the thread pool with unclosed resources. (as
		 * a safeguard against resource leaks, I suppose.)  
		 */
		private void initAsyncWorker() {
			new Thread("OMS Custom JMS producer-cached-connection creation thread") {
				@Override
				public void run() {
					while (true) {
						Object request = null;
						try {
							request = connectionRequestQueue.poll(1, TimeUnit.HOURS);
						} catch (InterruptedException ie) {
							logger.info(String.format("Received InterruptedException, %s will now stop", this.getName()));
							break;
						}
						if ( request == null ) {
							continue;
						}
						Object resource = null;
						if ( JMS_RESOURCE_CREATION_STOP.equals(request) ) {
							logger.info(String.format("Received stop request, %s will now stop", this.getName()));
							break;
						} else if (JMS_RESOURCE_CREATION_CONNECTION.equals(request) ){
							Connection conn = null;
							boolean closeConn = true;
							try {
								conn = sourceConnectionFactory.createConnection();
								Connection proxy = (Connection) Proxy.newProxyInstance(
										getClass().getClassLoader(), 
										new Class[]{ProducerCachedConnection.class}, 
										new ConnectionInvocationHandler(conn, connectionPool));
								Session session = proxy.createSession(false, Session.AUTO_ACKNOWLEDGE);
								session.createProducer(destination);
								logger.info("Created new JMS Connection (with cached session and producer)");
								resource = proxy;
								closeConn = false;
							} catch (JMSException e) {
								logger.error("Failed to create connection/session/producer", e);
								resource = e;
							} finally {
								if ( closeConn && conn != null ) {
									try {
										conn.close();
									} catch (JMSException e1) {
									}
								}
								if ( resource != null ) {
									connectionResponseQueue.add(resource);
								}
							}
						}
					}
				}
			}.start();
		}
	}

	private static class ConnectionInvocationHandler extends ResourceInvocationHandler {

		private Logger logger = LoggerFactory.getLogger(ConnectionInvocationHandler.class);
		public ConnectionInvocationHandler(Connection target, ObjectPool pool) {
			super(target, "createSession", "close", pool);
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			if ( "closeTargetConnection".equals(method.getName()) ) {
				((Connection) super.target).close();
				return null;
			}
			return super.invoke(proxy, method, args);
		}

		@Override
		protected void validateCreateMethodArguments(Object[] args) throws Throwable {
			if ( !( Boolean.FALSE.equals(args[0]) && new Integer(Session.AUTO_ACKNOWLEDGE).equals(args[1])) ) {
				UnsupportedOperationException e = new UnsupportedOperationException(
						"ProducerCachedConnectionFactory only supports creation of non-transacted and auto-acknowleged sessions!");
				e.fillInStackTrace();
				throw e;
			}
		}
		
		@Override
		protected Object createResourceProxy(Object resource) {
			return Proxy.newProxyInstance(
					getClass().getClassLoader(), 
					new Class[]{Session.class}, 
					new SessionInvocationHandler((Session) resource));
		}

		@Override
		protected void onCheckInException(Exception e) {
			logger.warn("Failed to return JMS Connection to OMS custom pool, will call close on the underlying connection", e);
			try {
				((Connection) super.target).close();
			} catch (JMSException jmsEx) {
				logger.warn("Failed to close JMS connection", jmsEx);
			}
			// TODO purge pool? re-throw? maybe this connection was checked out from an already purged pool?
			// don't re-throw
		}
	}

	private static class SessionInvocationHandler extends ResourceInvocationHandler {

		private Logger logger = LoggerFactory.getLogger(SessionInvocationHandler.class);
		
		private Map<Destination, MessageProducer> destinationProducerMap;
		public SessionInvocationHandler(Session session) {
			super(session, "createProducer", "close", null);
			destinationProducerMap = new HashMap<Destination, MessageProducer>();
		}

		@Override
		protected Object createResourceProxy(Object resource) {
			return Proxy.newProxyInstance(
					getClass().getClassLoader(), 
					new Class[]{MessageProducer.class}, 
					new ResourceInvocationHandler(resource, null, "close", null) );
		}
		
		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			if ( createMethodName.equals(method.getName()) && args.length == 1 && args[0] instanceof Destination ) {
				Destination destination = (Destination) args[0];
				MessageProducer cachedProducerProxy = destinationProducerMap.get(destination);
				if ( cachedProducerProxy == null ) {
					Object rawProducer = method.invoke(target, args);
					if ( rawProducer != null ) {
						cachedProducerProxy = (MessageProducer) createResourceProxy(rawProducer);
						destinationProducerMap.put(destination, cachedProducerProxy);
						if ( logger.isDebugEnabled() ) {
							logger.debug(String.format("Created NEW message producer proxy (%s) for destination (%s)", cachedProducerProxy, destination));
						}
					}
				} else if ( logger.isDebugEnabled() ) {
					logger.debug(String.format("Returning CACHED message producer proxy (%s) for destination (%s)", cachedProducerProxy, destination));
				}
				return cachedProducerProxy;
			}
			return super.invoke(proxy, method, args);
		}
	}
	
	private static class ResourceInvocationHandler implements InvocationHandler {
		
		private Logger logger = LoggerFactory.getLogger(ResourceInvocationHandler.class);

		protected Object target;
		protected String createMethodName;
		protected String closeMethodName;
		protected ObjectPool pool;
		protected Object cachedResource;

		
		public ResourceInvocationHandler(Object target,
				String createMethodName, String closeMethodName, ObjectPool pool) {
			this.target = target;
			this.createMethodName = createMethodName;
			this.closeMethodName = closeMethodName;
			this.pool = pool;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			try {
				if ( createMethodName != null && createMethodName.equals(method.getName()) ) {
					validateCreateMethodArguments(args);
					if ( cachedResource != null ) {
						return cachedResource;
					}
				} else if ( closeMethodName != null && closeMethodName.equals(method.getName()) ) {
					if ( pool != null ) {
						try {
							if ( logger.isDebugEnabled() ) {
								logger.debug(String.format("Returning pooled resource to pool... Resource: %s.  Current pool stats: %d active objects, %d idle objects",
										target, pool.getNumActive(), pool.getNumIdle() ));
							}
							pool.returnObject(proxy);
							if ( logger.isDebugEnabled() ) {
								logger.debug(String.format("Returned pooled resource.  Current pool stats: %d active objects, %d idle objects",
										pool.getNumActive(), pool.getNumIdle() ));
							}
						} catch (Exception e) {
							onCheckInException(e);
						}
					} // else simply keep it open
					return null;
				}
		
				// pass thru
				Object retVal = method.invoke(this.target, args);
		
				if ( createMethodName != null && createMethodName.equals(method.getName()) ) {
					cachedResource = createResourceProxy(retVal);
					return cachedResource;
				}
				return retVal;
			} catch (InvocationTargetException ex) {
				throw ex.getTargetException();
			}
		}
		
		protected Object createResourceProxy(Object resource) { return null; }
		protected void validateCreateMethodArguments(Object[] args) throws Throwable {}
		/**
		 * Exception handler for failure of returning the target object back to the pool 
		 */
		protected void onCheckInException(Exception e) {}
	}
	
	

}
