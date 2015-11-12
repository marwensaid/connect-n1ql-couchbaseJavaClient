/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.connect.n1ql.util;

import java.io.IOException;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryRow;
import com.couchbase.client.java.query.Statement;

/**
 * Abstract class for N1QL demos, where we init and cleanup a connection to the cluster
 */
public abstract class AbstractDemo {

    private static final String COUCHBASE_IP = "127.0.0.1";
    private static final String BUCKET = "beer-sample";
    private static final String PASSWORD = "";

    private static final CouchbaseEnvironment ENV = DefaultCouchbaseEnvironment.create();

    protected static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(AbstractDemo.class);

    protected final Cluster cluster;
    protected final Bucket bucket;

    protected AbstractDemo() {
        cluster = CouchbaseCluster.create(ENV, COUCHBASE_IP);
        try {
            bucket = cluster.openBucket(BUCKET, PASSWORD);
            demo();
        } finally {
            cluster.disconnect();
        }
    }

    public abstract void demo();

    public abstract String demoName();

    //issue a Query
    public void run(Query q, boolean shouldPause) {
        try {
            QueryResult result = bucket.query(q);
            printout(result, shouldPause);
        } catch (Exception e) {
            LOGGER.error("Error while issuing " + demoName(), e);
        }
    }

    public void run(Query q) {
        run(q, false);
    }

    public void run(Statement statement) {
        try {
            QueryResult result = bucket.query(statement);
            printout(result, false);
        } catch (Exception e) {
            LOGGER.error("Error while issuing statement " + demoName(), e);
        }
    }

    //printout query results
    public void printout(QueryResult result, boolean shouldPause) {
        if (result.finalSuccess()) {
            System.out.println(demoName() + " succeeded : " + result.allRows().size());
            pause(shouldPause);
            for (QueryRow queryRow : result) {
                System.out.println(queryRow.value());
            }
        } else {
            System.err.println(demoName() + " failed ");
            for (JsonObject error : result.errors()) {
                System.err.println(error);
            }
        }
    }

    private void pause(boolean shouldPause) {
        try {
            if (shouldPause) {
                System.in.read();
            }
        } catch (IOException e) {
            LOGGER.error("pause error", e);
        }
    }
}
