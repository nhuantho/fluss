/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.lance.utils;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.BigIntType;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.BooleanType;
import org.apache.fluss.types.BytesType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypeVisitor;
import org.apache.fluss.types.DateType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.DoubleType;
import org.apache.fluss.types.FloatType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.SmallIntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.types.TimeType;
import org.apache.fluss.types.TimestampType;
import org.apache.fluss.types.TinyIntType;

/**
 * Convert from Fluss's data type to Arrow's data type.
 */
public class FlussDataTypeArrowType implements DataTypeVisitor<ArrowType> {

    public static final FlussDataTypeArrowType INSTANCE =
            new FlussDataTypeArrowType();

    @Override
    public ArrowType visit(CharType charType) {
        return ArrowType.Utf8.INSTANCE;
    }

    @Override
    public ArrowType visit(StringType stringType) {
        return ArrowType.Utf8.INSTANCE;
    }

    @Override
    public ArrowType visit(BooleanType booleanType) {
        return ArrowType.Bool.INSTANCE;
    }

    @Override
    public ArrowType visit(BinaryType binaryType) {
        return new ArrowType.FixedSizeBinary(binaryType.getLength());
    }

    @Override
    public ArrowType visit(BytesType bytesType) {
        return ArrowType.Binary.INSTANCE;
    }

    @Override
    public ArrowType visit(DecimalType decimalType) {
        return ArrowType.Decimal.createDecimal(
                decimalType.getPrecision(), decimalType.getScale(), null);
    }

    @Override
    public ArrowType visit(TinyIntType tinyIntType) {
        return new ArrowType.Int(8, true);
    }

    @Override
    public ArrowType visit(SmallIntType smallIntType) {
        return new ArrowType.Int(16, true);
    }

    @Override
    public ArrowType visit(IntType intType) {
        return new ArrowType.Int(32, true);
    }

    @Override
    public ArrowType visit(BigIntType bigIntType) {
        return new ArrowType.Int(64, true);
    }

    @Override
    public ArrowType visit(FloatType floatType) {
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    }

    @Override
    public ArrowType visit(DoubleType doubleType) {
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    }

    @Override
    public ArrowType visit(DateType dateType) {
        return new ArrowType.Date(DateUnit.DAY);
    }

    @Override
    public ArrowType visit(TimeType timeType) {
        if (timeType.getPrecision() == 0) {
            return new ArrowType.Time(TimeUnit.SECOND, 32);
        } else if (timeType.getPrecision() >= 1 && timeType.getPrecision() <= 3) {
            return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
        } else if (timeType.getPrecision() >= 4 && timeType.getPrecision() <= 6) {
            return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
        } else {
            return new ArrowType.Time(TimeUnit.NANOSECOND, 64);
        }
    }

    @Override
    public ArrowType visit(TimestampType timestampType) {
        if (timestampType.getPrecision() == 0) {
            return new ArrowType.Timestamp(TimeUnit.SECOND, null);
        } else if (timestampType.getPrecision() >= 1 && timestampType.getPrecision() <= 3) {
            return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
        } else if (timestampType.getPrecision() >= 4 && timestampType.getPrecision() <= 6) {
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        } else {
            return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
        }
    }

    @Override
    public ArrowType visit(LocalZonedTimestampType localZonedTimestampType) {
        if (localZonedTimestampType.getPrecision() == 0) {
            return new ArrowType.Timestamp(TimeUnit.SECOND, null);
        } else if (localZonedTimestampType.getPrecision() >= 1 && localZonedTimestampType.getPrecision() <= 3) {
            return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
        } else if (localZonedTimestampType.getPrecision() >= 4 && localZonedTimestampType.getPrecision() <= 6) {
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        } else {
            return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
        }
    }

    @Override
    public ArrowType visit(ArrayType arrayType) {
        return ArrowType.List.INSTANCE;
    }

    @Override
    public ArrowType visit(MapType mapType) {
        return ArrowType.Map;
    }

    @Override
    public ArrowType visit(RowType rowType) {
        org.apache.paimon.types.RowType.Builder rowTypeBuilder =
                org.apache.paimon.types.RowType.builder();
        for (DataField field : rowType.getFields()) {
            rowTypeBuilder.field(
                    field.getName(),
                    field.getType().accept(this),
                    field.getDescription().orElse(null));
        }
        return withNullability(rowTypeBuilder.build(), rowType.isNullable());
    }
}
