/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.jms.spout;

import org.apache.storm.jms.JmsProvider;
import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A Storm <code>Spout</code> implementation that listens to a JMS topic or
 * queue and outputs tuples based on the messages it receives.
 *
 * <p><code>JmsSpout</code> instances rely on <code>JmsProducer</code>
 * implementations to obtain the JMS
 * <code>ConnectionFactory</code> and <code>Destination</code> objects necessary
 * to connect to a JMS topic/queue.
 *
 * <p>When a {@code JmsSpout} receives a JMS message, it delegates to an
 * internal {@code JmsTupleProducer} instance to create a Storm tuple from
 * the incoming message.
 *
 * <p>Typically, developers will supply a custom <code>JmsTupleProducer</code>
 * implementation appropriate for the expected message content.
 */
@SuppressWarnings("serial")
public class JmsSpout extends BaseRichSpout implements MessageListener {

    /** The logger object instance for this class. */
    private static final Logger LOG = LoggerFactory.getLogger(JmsSpout.class);

    /** Time to sleep between queue polling attempts. */
    private static final int POLL_INTERVAL_MS = 50;

    /**
     * The acknowledgment mode used for this instance.
     *
     * @see Session
     */
    private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;

    /** Indicates whether or not this spout should run as a singleton. */
    private boolean distributed = true;

    /** Used to generate tuples from incoming messages. */
    private JmsTupleProducer tupleProducer;

    /** Encapsulates jms related classes needed to communicate with the mq. */
    private JmsProvider jmsProvider;

    /** Stores incoming messages for later sending. */
    private LinkedBlockingQueue<Message> queue;

    /** Counter of handled messages. */
    private long messageSequence = 0;

    /** The collector used to emit tuples. */
    private SpoutOutputCollector collector;

    /** Connection to the jms queue. */
    private transient Connection connection;

    /** The active jms session. */
    private transient Session session;

    /** Sets up the way we want to handle the emit, ack and fails. */
    private MessageHandler messageHandler;

    /**
     * Does ack-ing a JMS message ack only an individual message
     * Some JMS providers allow users to set this behavior.
     */
    private boolean individualAcks;

    /**
     * Sets the JMS Session acknowledgement mode for the JMS session.
     *
     * <p>Possible values:
     * <ul>
     * <li>javax.jms.Session.AUTO_ACKNOWLEDGE</li>
     * <li>javax.jms.Session.CLIENT_ACKNOWLEDGE</li>
     * <li>javax.jms.Session.DUPS_OK_ACKNOWLEDGE</li>
     * </ul>
     *
     * @param mode JMS Session Acknowledgement mode
     * @throws IllegalArgumentException if the mode is not recognized.
     */
    public void setJmsAcknowledgeMode(final int mode) {
        switch (mode) {
            case Session.AUTO_ACKNOWLEDGE:
            case Session.DUPS_OK_ACKNOWLEDGE:
                messageHandler = new MessageHandler();
                break;
            case Session.CLIENT_ACKNOWLEDGE:
            case Session.SESSION_TRANSACTED:
                if (individualAcks) {
                    messageHandler = new MessageAckHandler();
                } else {
                    messageHandler = new ClientAckHandler();
                }
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown Acknowledge mode: " + mode
                            + " (See javax.jms.Session for valid values)");

        }
        this.jmsAcknowledgeMode = mode;
    }

    /**
     * Returns the JMS Session acknowledgement mode for the JMS session
     * associated with this spout. Can be either of:
     * <ul>
     * <li>{@link Session#AUTO_ACKNOWLEDGE}</li>
     * <li>{@link Session#CLIENT_ACKNOWLEDGE}</li>
     * <li>{@link Session#DUPS_OK_ACKNOWLEDGE}</li>
     * <li>{@link Session#SESSION_TRANSACTED}</li>
     * </ul>
     *
     * @return the int value of the acknowledgment mode.
     */
    public int getJmsAcknowledgeMode() {
        return this.jmsAcknowledgeMode;
    }

    /**
     * Set {@link #jmsProvider}.
     *
     * <p>Set the <code>JmsProvider</code>
     * implementation that this Spout will use to connect to
     * a JMS <code>javax.jms.Desination</code>
     *
     * @param provider the provider to use
     */
    public void setJmsProvider(final JmsProvider provider) {
        this.jmsProvider = provider;
    }

    /**
     * Set the <code>JmsTupleProducer</code>
     * implementation that will convert <code>javax.jms.Message</code>
     * object to <code>org.apache.storm.tuple.Values</code> objects
     * to be emitted.
     *
     * @param producer the producer instance to use
     */
    public void setJmsTupleProducer(final JmsTupleProducer producer) {
        this.tupleProducer = producer;
    }

    /**
     * Set if JMS provider acks individual messages.
     */
    public void setIndividualAcks() {
        individualAcks = true;
    }
    /**
     * <code>javax.jms.MessageListener</code> implementation.
     *
     * <p>Stored the JMS message in an internal queue for processing
     * by the <code>nextTuple()</code> method.
     *
     * @param msg the message to handle
     */
    public void onMessage(final Message msg) {
        try {
            messageHandler.lock();
            LOG.debug("Queuing msg [" + msg.getJMSMessageID() + "]");
            queue.offer(msg);
        } catch (JMSException ignored) {
            // Do nothing
        } finally {
            messageHandler.unlock();
        }
    }

    /**
     * <code>ISpout</code> implementation.
     *
     * <p>Connects the JMS spout to the configured JMS destination
     * topic/queue.
     */
    @Override
    public void open(final Map<String, Object> conf,
                     final TopologyContext context,
                     final SpoutOutputCollector spoutOutputCollector) {

        if (this.jmsProvider == null) {
            throw new IllegalStateException(
                "JMS provider has not been set.");
        }
        if (this.tupleProducer == null) {
            throw new IllegalStateException(
                "JMS Tuple Producer has not been set.");
        }
        queue = new LinkedBlockingQueue<>();
        collector = spoutOutputCollector;
        try {
            ConnectionFactory cf = jmsProvider.connectionFactory();
            Destination dest = jmsProvider.destination();
            connection = cf.createConnection();
            session = connection.createSession(false, jmsAcknowledgeMode);
            MessageConsumer consumer = session.createConsumer(dest);
            consumer.setMessageListener(this);
            connection.start();
        } catch (Exception e) {
            LOG.warn("Error creating JMS connection.", e);
        }

    }

    /**
     * Close the {@link #session} and {@link #connection}.
     *
     * <p>When overridden, should always call {@code super}
     * to finalize the active connections.
     */
    public void close() {
        try {
            LOG.debug("Closing JMS connection.");
            session.close();
            connection.close();
        } catch (JMSException e) {
            LOG.warn("Error closing JMS connection.", e);
        }

    }

    /**
     * Generate the next tuple from a message.
     *
     * <p>This method polls the queue that's being filled asynchronously by the
     * jms connection, every {@link #POLL_INTERVAL_MS} seconds.
     */
    public void nextTuple() {
        try {
            messageHandler.lock();
            Message msg = queue.poll(POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
            LOG.debug("sending tuple {}", msg);
            // get the tuple from the handler
            messageHandler.emit(msg);
        } catch (InterruptedException ex) {
          LOG.warn("Got error trying to process tuple", ex);
        } finally {
            messageHandler.unlock();
        }
    }

    /**
     * Ack a successfully handled message by the matching {@link JmsMessageID}.
     *
     * <p>Acking means removing the message from the pending messages
     * collections, and if it was the oldest pending message -
     * ack it to the mq as well, so that it's the only one acked.
     *
     * <p>Will only be called if we're transactional or not AUTO_ACKNOWLEDGE.
     */
    @Override
    public void ack(final Object msgId) {
        LOG.debug("Received ACK for message: {}", msgId);
        try {
            messageHandler.lock();
            messageHandler.ack(msgId);
        } finally {
            messageHandler.unlock();
        }
    }

    /**
     * Fail an unsuccessfully handled message by its {@link JmsMessageID}.
     *
     * <p>Failing means dropping all pending messages and queueing a recovery
     * attempt.
     *
     * <p>Will only be called if we're transactional or not AUTO_ACKNOWLEDGE
     */
    @Override
    public void fail(final Object msgId) {
        LOG.warn("Message failed: " + msgId);
        try {
            messageHandler.lock();
            messageHandler.fail(msgId);
        } finally {
            messageHandler.unlock();
        }
    }

    /**
     * Use the {@link #tupleProducer} to determine which fields are about to be emitted.
     *
     * <p>Note that {@link #nextTuple()} always emits to the default stream, and thus only fields declared
     * for this stream are used.
     */
    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        this.tupleProducer.declareOutputFields(declarer);

    }

    /**
     * Sets the periodicity of the timer task that
     * checks for failures and recovers the JMS session.
     *
     * @param period desired wait period
     */
    public void setRecoveryPeriodMs(final long period) {
    }

    /**
     * @return {@link #distributed}.
     */
    public boolean isDistributed() {
        return this.distributed;
    }

    /**
     * Sets the "distributed" mode of this spout.
     *
     * <p>If <code>true</code> multiple instances of this spout <i>may</i> be
     * created across the cluster
     * (depending on the "parallelism_hint" in the topology configuration).
     *
     * <p>Setting this value to <code>false</code> essentially means this spout
     * will run as a singleton within the cluster
     * ("parallelism_hint" will be ignored).
     *
     * <p>In general, this should be set to <code>false</code> if the underlying
     * JMS destination is a topic, and <code>true</code> if it is a JMS queue.
     *
     * @param isDistributed {@code true} if should be distributed, {@code false}
     *                      otherwise.
     */
    public void setDistributed(final boolean isDistributed) {
        this.distributed = isDistributed;
    }

    /**
     * @return The currently active session.
     */
    protected Session getSession() {
        return session;
    }

    /**
     * Handles messages in JMS AUTO or DUPS_OK ack mode.
     */
    private class MessageHandler {

        void emit(final Message msg) {
            LOG.debug("Received msg {}", msg);
            try {
                Values vals = tupleProducer.toTuple(msg);
                collector.emit(vals);
            } catch (JMSException ex) {
                LOG.warn("Error processing message {}", msg);
            }
        }

        void ack(final Object msgId) {
            // NOOP
        }

        void fail(final Object msgId) {
            // NOOP
        }

        void lock() {
            // NOOP
        }

        void unlock() {
            // NOOP
        }
    }

    /**
     * JMS mode where individual messages can be ack-ed
     */
    private class MessageAckHandler extends MessageHandler {
        /** Maps between message ids of not-yet acked messages, and the messages. */
        protected HashMap<JmsMessageID, Message> pendingAcks = new HashMap<>();

        @Override
        void emit(final Message msg) {
            LOG.debug("Received msg {}, Requesting acks.", msg);
            try {
                JmsMessageID messageId = new JmsMessageID(messageSequence++, msg.getJMSMessageID());
                Values vals = tupleProducer.toTuple(msg);
                collector.emit(vals, messageId);
                pendingAcks.put(messageId, msg);
            } catch (JMSException ex) {
                LOG.warn("Error processing message {}", msg);
            }
        }

        @Override
        void ack(final Object msgId) {
            if (pendingAcks.isEmpty()) {
                LOG.debug("Not processing the ACK, pendingAcks is empty");
            } else {
                Message msg = pendingAcks.remove(msgId);
                if (msg != null) {
                    try {
                        doAck(msg);
                        LOG.debug("JMS Message acked: {}", msgId);
                    } catch (JMSException e) {
                        LOG.warn("Error acknowledging JMS message: {}", msgId, e);
                    }

                } else {
                    LOG.warn("Couldn't acknowledge unknown JMS message: {}", msgId);
                }
            }
        }

        @Override
        void fail(final Object msgId) {
            LOG.debug("Received fail for message {}", msgId);
            try {
                doFail(msgId);
            } catch (JMSException ex) {
                LOG.warn("Error during session recovery", ex);
            }
        }

        protected void doAck(Message msg) throws JMSException {
            msg.acknowledge();
        }

        protected void doFail(final Object msgId) throws JMSException {
            pendingAcks.remove(msgId);
            getSession().recover();
        }
    }

    private class ClientAckHandler extends MessageAckHandler {
        private Lock lock = new ReentrantLock(true);

        @Override
        protected void doAck(final Message msg) throws JMSException {
            // if there are no more pending consumed messages and storm delivered ack for all
            if (pendingAcks.isEmpty() && queue.isEmpty()) {
                msg.acknowledge();
            } else {
                LOG.debug("Not acknowledging the JMS message since there are pending messages in the session");
            }
        }

        @Override
        protected void doFail(final Object msgId) throws JMSException {
            if (!pendingAcks.isEmpty()) {
                pendingAcks.clear();
                getSession().recover();
            }
        }


        @Override
        void lock() {
            lock.lock();
        }

        @Override
        void unlock() {
            lock.unlock();
        }

    }
}
