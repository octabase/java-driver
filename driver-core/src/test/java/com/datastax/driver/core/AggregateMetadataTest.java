/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.text;

@CassandraVersion(major = 2.2)
@CCMConfig(config = "enable_user_defined_functions:true")
public class AggregateMetadataTest extends CCMTestsSupport {

    @Test(groups = "short")
    public void should_parse_and_format_aggregate_with_initcond_and_no_finalfunc() {
        // given
        String cqlFunction = String.format("CREATE FUNCTION %s.cat(s text,v int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return s+v;';", keyspace);
        String cqlAggregate = String.format("CREATE AGGREGATE %s.cat_tos(int) SFUNC cat STYPE text INITCOND '0';", keyspace);
        // when
        session().execute(cqlFunction);
        session().execute(cqlAggregate);
        // then
        KeyspaceMetadata keyspace = cluster().getMetadata().getKeyspace(this.keyspace);
        FunctionMetadata stateFunc = keyspace.getFunction("cat", text(), cint());
        AggregateMetadata aggregate = keyspace.getAggregate("cat_tos", cint());
        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getSignature()).isEqualTo("cat_tos(int)");
        assertThat(aggregate.getSimpleName()).isEqualTo("cat_tos");
        assertThat(aggregate.getArgumentTypes()).containsExactly(cint());
        assertThat(aggregate.getFinalFunc()).isNull();
        assertThat(aggregate.getInitCond()).isEqualTo("0");
        assertThat(aggregate.getReturnType()).isEqualTo(text());
        assertThat(aggregate.getStateFunc()).isEqualTo(stateFunc);
        assertThat(aggregate.getStateType()).isEqualTo(text());
        assertThat(aggregate.toString()).isEqualTo(cqlAggregate);
        assertThat(aggregate.exportAsString()).isEqualTo(String.format("CREATE AGGREGATE %s.cat_tos(int)\n"
                + "SFUNC cat STYPE text\n"
                + "INITCOND '0';", this.keyspace));
    }

    @Test(groups = "short")
    public void should_parse_and_format_aggregate_with_no_arguments() {
        // given
        String cqlFunction = String.format("CREATE FUNCTION %s.inc(i int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return i+1;';", keyspace);
        String cqlAggregate = String.format("CREATE AGGREGATE %s.mycount() SFUNC inc STYPE int INITCOND 0;", keyspace);
        // when
        session().execute(cqlFunction);
        session().execute(cqlAggregate);
        // then
        KeyspaceMetadata keyspace = cluster().getMetadata().getKeyspace(this.keyspace);
        FunctionMetadata stateFunc = keyspace.getFunction("inc", cint());
        AggregateMetadata aggregate = keyspace.getAggregate("mycount");
        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getSignature()).isEqualTo("mycount()");
        assertThat(aggregate.getSimpleName()).isEqualTo("mycount");
        assertThat(aggregate.getArgumentTypes()).isEmpty();
        assertThat(aggregate.getFinalFunc()).isNull();
        assertThat(aggregate.getInitCond()).isEqualTo(0);
        assertThat(aggregate.getReturnType()).isEqualTo(cint());
        assertThat(aggregate.getStateFunc()).isEqualTo(stateFunc);
        assertThat(aggregate.getStateType()).isEqualTo(cint());
        assertThat(aggregate.toString()).isEqualTo(cqlAggregate);
        assertThat(aggregate.exportAsString()).isEqualTo(String.format("CREATE AGGREGATE %s.mycount()\n"
                + "SFUNC inc STYPE int\n"
                + "INITCOND 0;", this.keyspace));
    }

    @Test(groups = "short")
    public void should_parse_and_format_aggregate_with_final_function() {
        // given
        String cqlFunction1 = String.format("CREATE FUNCTION %s.plus(i int, j int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return i+j;';", keyspace);
        String cqlFunction2 = String.format("CREATE FUNCTION %s.announce(i int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return i;';", keyspace);
        String cqlAggregate = String.format("CREATE AGGREGATE %s.prettysum(int) SFUNC plus STYPE int FINALFUNC announce INITCOND 0;", keyspace);
        // when
        session().execute(cqlFunction1);
        session().execute(cqlFunction2);
        session().execute(cqlAggregate);
        // then
        KeyspaceMetadata keyspace = cluster().getMetadata().getKeyspace(this.keyspace);
        FunctionMetadata stateFunc = keyspace.getFunction("plus", cint(), cint());
        FunctionMetadata finalFunc = keyspace.getFunction("announce", cint());
        AggregateMetadata aggregate = keyspace.getAggregate("prettysum", cint());
        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getSignature()).isEqualTo("prettysum(int)");
        assertThat(aggregate.getSimpleName()).isEqualTo("prettysum");
        assertThat(aggregate.getArgumentTypes()).containsExactly(cint());
        assertThat(aggregate.getFinalFunc()).isEqualTo(finalFunc);
        assertThat(aggregate.getInitCond()).isEqualTo(0);
        assertThat(aggregate.getReturnType()).isEqualTo(cint());
        assertThat(aggregate.getStateFunc()).isEqualTo(stateFunc);
        assertThat(aggregate.getStateType()).isEqualTo(cint());
        assertThat(aggregate.toString()).isEqualTo(cqlAggregate);
        assertThat(aggregate.exportAsString()).isEqualTo(String.format("CREATE AGGREGATE %s.prettysum(int)\n"
                + "SFUNC plus STYPE int\n"
                + "FINALFUNC announce\n"
                + "INITCOND 0;", this.keyspace));
    }

    @Test(groups = "short")
    public void should_parse_and_format_aggregate_with_no_initcond() {
        // given
        String cqlFunction = String.format("CREATE FUNCTION %s.plus2(i int, j int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return i+j;';", keyspace);
        String cqlAggregate = String.format("CREATE AGGREGATE %s.sum(int) SFUNC plus2 STYPE int;", keyspace);
        // when
        session().execute(cqlFunction);
        session().execute(cqlAggregate);
        // then
        KeyspaceMetadata keyspace = cluster().getMetadata().getKeyspace(this.keyspace);
        FunctionMetadata stateFunc = keyspace.getFunction("plus2", cint(), cint());
        AggregateMetadata aggregate = keyspace.getAggregate("sum", cint());
        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getSignature()).isEqualTo("sum(int)");
        assertThat(aggregate.getSimpleName()).isEqualTo("sum");
        assertThat(aggregate.getArgumentTypes()).containsExactly(cint());
        assertThat(aggregate.getFinalFunc()).isNull();
        assertThat(aggregate.getInitCond()).isNull();
        assertThat(aggregate.getReturnType()).isEqualTo(cint());
        assertThat(aggregate.getStateFunc()).isEqualTo(stateFunc);
        assertThat(aggregate.getStateType()).isEqualTo(cint());
        assertThat(aggregate.toString()).isEqualTo(cqlAggregate);
        assertThat(aggregate.exportAsString()).isEqualTo(String.format("CREATE AGGREGATE %s.sum(int)\n"
                + "SFUNC plus2 STYPE int;", this.keyspace));
    }

    @Test(groups = "short")
    public void should_parse_and_format_aggregate_with_udts() {
        // given
        String cqlFunction = String.format(
                "CREATE FUNCTION %s.\"MY_FUNC\"(address1 \"Address\", address2 \"Address\") "
                        + "CALLED ON NULL INPUT "
                        + "RETURNS \"Address\" "
                        + "LANGUAGE java "
                        + "AS 'return address1;'", keyspace);
        String cqlAggregate = String.format(
                "CREATE AGGREGATE %s.\"MY_AGGREGATE\"(\"Address\") "
                        + "SFUNC \"MY_FUNC\" "
                        + "STYPE \"Address\";",
                keyspace);
        // when
        session().execute(cqlFunction);
        session().execute(cqlAggregate);
        // then
        KeyspaceMetadata keyspace = cluster().getMetadata().getKeyspace(this.keyspace);
        UserType addressType = keyspace.getUserType("\"Address\"");
        FunctionMetadata stateFunc = keyspace.getFunction("\"MY_FUNC\"", addressType, addressType);
        AggregateMetadata aggregate = keyspace.getAggregate("\"MY_AGGREGATE\"", addressType);
        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getSignature()).isEqualTo("\"MY_AGGREGATE\"(\"Address\")");
        assertThat(aggregate.getSimpleName()).isEqualTo("MY_AGGREGATE");
        assertThat(aggregate.getArgumentTypes()).containsExactly(addressType);
        assertThat(aggregate.getFinalFunc()).isNull();
        assertThat(aggregate.getInitCond()).isNull();
        assertThat(aggregate.getReturnType()).isEqualTo(addressType);
        assertThat(aggregate.getStateFunc()).isEqualTo(stateFunc);
        assertThat(aggregate.getStateType()).isEqualTo(addressType);
        assertThat(aggregate.toString()).isEqualTo(cqlAggregate);
    }

    /**
     * Test for JAVA-1046 / CASSANDRA-11064.
     * <p/>
     * TODO enable this test when CASSANDRA-11064 is fixed.
     */
    @Test(groups = "short", enabled = false)
    public void should_parse_and_format_aggregate_with_composite_type() {
        // given
        DataType custom = DataType.custom(
                "org.apache.cassandra.db.marshal.DynamicCompositeType(" +
                        "s=>org.apache.cassandra.db.marshal.UTF8Type," +
                        "i=>org.apache.cassandra.db.marshal.Int32Type)");
        String cqlFunction = String.format(
                "CREATE FUNCTION %s.id(i %s) " +
                        "RETURNS NULL ON NULL INPUT " +
                        "RETURNS %s " +
                        "LANGUAGE java " +
                        "AS 'return i;';", keyspace, custom, custom);
        String cqlAggregate = String.format(
                "CREATE AGGREGATE %s.ag() " +
                        "SFUNC id " +
                        "STYPE %s " +
                        "INITCOND 's@foo:i@32';", keyspace, custom);
        // when
        session().execute(cqlFunction);
        session().execute(cqlAggregate);
        // then
        KeyspaceMetadata keyspace = cluster().getMetadata().getKeyspace(this.keyspace);
        FunctionMetadata stateFunc = keyspace.getFunction("id", custom);
        AggregateMetadata aggregate = keyspace.getAggregate("ag");
        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getSignature()).isEqualTo("ag()");
        assertThat(aggregate.getSimpleName()).isEqualTo("ag");
        assertThat(aggregate.getArgumentTypes()).isEmpty();
        assertThat(aggregate.getFinalFunc()).isNull();
        assertThat(aggregate.getInitCond()).isEqualTo("'s@foo:i@32'");
        assertThat(aggregate.getReturnType()).isEqualTo(custom);
        assertThat(aggregate.getStateFunc()).isEqualTo(stateFunc);
        assertThat(aggregate.getStateType()).isEqualTo(custom);
        assertThat(aggregate.toString()).isEqualTo(cqlAggregate);
    }

    @Override
    public void onTestContextInitialized() {
        execute(
                String.format("CREATE TYPE IF NOT EXISTS %s.phone (number text)", keyspace),
                String.format("CREATE TYPE IF NOT EXISTS %s.\"Address\" ("
                        + "    street text,"
                        + "    city text,"
                        + "    zip int,"
                        + "    phones frozen<set<frozen<phone>>>,"
                        + "    location frozen<tuple<float, float>>"
                        + ")", keyspace)
        );
    }

}
