package org.oliverlittle.clusterprocess.model.table.field

import org.oliverlittle.clusterprocess.UnitSpec
import org.oliverlittle.clusterprocess.table_model
import org.oliverlittle.clusterprocess.table_model.DataType.STRING
import org.oliverlittle.clusterprocess.table_model.DataType.DATETIME
import org.oliverlittle.clusterprocess.table_model.DataType.BOOL
import org.oliverlittle.clusterprocess.table_model.DataType.DOUBLE
import org.oliverlittle.clusterprocess.table_model.DataType.INT
import org.oliverlittle.clusterprocess.model.table.field.TableValue.protobuf

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import java.time.Instant

class TableFieldTest extends UnitSpec {
    "A TableField" should "convert from protobuf to BaseTableField" in {
        val results = Seq(
            (table_model.TableResultHeader.Header("x", STRING), BaseStringField("x")),
            (table_model.TableResultHeader.Header("x", DATETIME), BaseDateTimeField("x")),
            (table_model.TableResultHeader.Header("x", BOOL), BaseBoolField("x")),
            (table_model.TableResultHeader.Header("x", DOUBLE), BaseDoubleField("x")),
            (table_model.TableResultHeader.Header("x", INT), BaseIntField("x")),
        )

        results.foreach((i, o) =>
            TableField.fromProtobuf(i) should be (o)    
        )
    }
}

class TableValueTest extends UnitSpec {
    "A TableValue" should "convert from protobuf" in {
        val results = Seq(
            (table_model.Value(table_model.Value.Value.String("x")), Some(StringValue("x"))),
            (table_model.Value(table_model.Value.Value.Datetime("2023-02-28T08:09:38Z")), Some(DateTimeValue(Instant.parse("2023-02-28T08:09:38Z")))),
            (table_model.Value(table_model.Value.Value.Bool(true)), Some(BoolValue(true))),
            (table_model.Value(table_model.Value.Value.Double(1.01d)), Some(DoubleValue(1.01d))),
            (table_model.Value(table_model.Value.Value.Int(1L)), Some(IntValue(1L))),
            (table_model.Value(table_model.Value.Value.Null(true)), None)
        )

        results.foreach((i, o) =>
            TableValue.fromProtobuf(i) should be (o)    
        )
    }

    it should "convert to protobuf when it has a value" in {
        Some(StringValue("x")).protobuf should be (table_model.Value(table_model.Value.Value.String("x")))
    }

    it should "convert to a null protobuf value when it is None" in {
        None.protobuf should be (table_model.Value(table_model.Value.Value.Null(true)))
    }
}