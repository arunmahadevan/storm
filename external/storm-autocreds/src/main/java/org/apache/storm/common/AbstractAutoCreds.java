package org.apache.storm.common;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.storm.hdfs.security.AutoHDFS;
import org.apache.storm.hdfs.security.HdfsSecurityUtil;
import org.apache.storm.security.INimbusCredentialPlugin;
import org.apache.storm.security.auth.IAutoCredentials;
import org.apache.storm.security.auth.ICredentialsRenewer;
import org.apache.storm.streams.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractAutoCreds implements IAutoCredentials, ICredentialsRenewer, INimbusCredentialPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractAutoCreds.class);

    private List<String> configKeys;

    @Override
    public void prepare(Map conf) {
        doPrepare(conf);
        String configKeyString = getConfigKeyString();
        if (conf.containsKey(configKeyString)) {
            configKeys = (List<String>) conf.get(configKeyString);
        }
    }

    @Override
    public void populateCredentials(Map<String, String> credentials, Map conf) {
        try {
            if (configKeys != null) {
                for (String configKey : configKeys) {
                    credentials.put(getCredentialKey(configKey),
                            DatatypeConverter.printBase64Binary(getHadoopCredentials(conf, configKey)));
                }
            } else {
                credentials.put(getCredentialKey(StringUtils.EMPTY),
                        DatatypeConverter.printBase64Binary(getHadoopCredentials(conf)));
            }
            LOG.info("Tokens added to credentials map.");
        } catch (Exception e) {
            LOG.error("Could not populate credentials.", e);
        }
    }

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        credentials.put(getCredentialKey(StringUtils.EMPTY),
                DatatypeConverter.printBase64Binary("dummy place holder".getBytes()));
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        addCredentialToSubject(subject, credentials);
        addTokensToUGI(subject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        addCredentialToSubject(subject, credentials);
        addTokensToUGI(subject);
    }

    protected Set<Pair<String, Credentials>> getCredentials(Map<String, String> credentials) {
        Set<Pair<String, Credentials>> res = new HashSet<>();
        if (configKeys != null) {
            for (String configKey: configKeys) {
                Credentials cred = doGetCredentials(credentials, configKey);
                if (cred != null) {
                    res.add(Pair.of(configKey, cred));
                }
            }
        } else {
            Credentials cred = doGetCredentials(credentials, StringUtils.EMPTY);
            if (cred != null) {
                res.add(Pair.of(StringUtils.EMPTY, cred));
            }
        }
        return res;
    }

    protected abstract void doPrepare(Map conf);

    protected abstract String getConfigKeyString();

    protected abstract String getCredentialKey(String configKey);

    protected abstract byte[] getHadoopCredentials(Map conf, String configKey);

    protected abstract byte[] getHadoopCredentials(Map conf);

    @SuppressWarnings("unchecked")
    private void addCredentialToSubject(Subject subject, Map<String, String> credentials) {
        try {
            for (Pair<String, Credentials> cred : getCredentials(credentials)) {
                subject.getPrivateCredentials().add(cred.getSecond());
                LOG.info("Credentials added to the subject.");
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize and get UserGroupInformation.", e);
        }
    }

    private void addTokensToUGI(Subject subject) {
        if(subject != null) {
            Set<Credentials> privateCredentials = subject.getPrivateCredentials(Credentials.class);
            if (privateCredentials != null) {
                for (Credentials cred : privateCredentials) {
                    Collection<Token<? extends TokenIdentifier>> allTokens = cred.getAllTokens();
                    if (allTokens != null) {
                        for (Token<? extends TokenIdentifier> token : allTokens) {
                            try {
                                UserGroupInformation.getCurrentUser().addToken(token);
                                LOG.info("Added delegation tokens to UGI.");
                            } catch (IOException e) {
                                LOG.error("Exception while trying to add tokens to ugi", e);
                            }
                        }
                    }
                }
            }
        }
    }

    private Credentials doGetCredentials(Map<String, String> credentials, String configKey) {
        Credentials credential = null;
        if (credentials != null && credentials.containsKey(getCredentialKey(configKey))) {
            try {
                byte[] credBytes = DatatypeConverter.parseBase64Binary(credentials.get(getCredentialKey(configKey)));
                ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(credBytes));

                credential = new Credentials();
                credential.readFields(in);
            } catch (Exception e) {
                LOG.error("Could not obtain credentials from credentials map.", e);
            }
        }
        return credential;

    }

}
