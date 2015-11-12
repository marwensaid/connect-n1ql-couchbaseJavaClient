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
package com.couchbase.connect.n1ql;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;
import static com.couchbase.client.java.query.dsl.Expression.s;
import static com.couchbase.client.java.query.dsl.Expression.x;

import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.functions.StringFunctions;
import com.couchbase.connect.n1ql.util.AbstractDemo;

public class DJoinDemo extends AbstractDemo {

    public static void main(String[] args) {
        new DJoinDemo();
    }

    @Override
    public String demoName() {
        return "join query";
    }

    @Override
    public void demo() {
        //JOIN beers and breweries (show beer name, brewery name, brewery website)
        //limit to brewery with name starting with "Flying..."
        Statement joinPredicate = select(x("beers.name").as("beer"),
                x("brewery.name").as("brewery_name"),
                x("brewery.website").as("website"))
                .from(i("beer-sample").as("beers"))
                .join(i("beer-sample").toString()).as("brewery")
                .onKeys(x("beers.brewery_id"))
                .where(StringFunctions.substr("brewery.name", 0, 6).eq(s("Flying")));

        System.out.println(joinPredicate.toString());

        Query query = Query.simple(joinPredicate);

        run(query, true);
    }
}
