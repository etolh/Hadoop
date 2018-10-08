package com.huliang.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * 处理Null值UDF：接收2个参数，若第一个为null，返回第二次参数，否则返回第一个参数
 * @author huliang
 * @date 2018/10/8 16:25
 */

@Description(name = "nvl",
        value = "_FUNC_(value,default_value) - Returns default value if value"
                +" is null else returns value",
        extended = "Example:\n"
                + " > SELECT _FUNC_(null,'bla') FROM src LIMIT 1;\n")
public class GenericUDFNvl extends GenericUDF {

    private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    private ObjectInspector[] argOIs;

    // 确定evaluate参数的返回类型
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        argOIs = arguments;
        if(arguments.length != 2) {
            throw new UDFArgumentLengthException(
                    "The operator 'NVL' accepts 2 arguments.");
        }
        returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
        //检查参数类型
        if (!(returnOIResolver.update(arguments[0]) && returnOIResolver
                .update(arguments[1]))) {
            throw new UDFArgumentTypeException(2,
                    "The 1st and 2nd args of function NLV should have the same type, "
                            + "but they are different: \"" + arguments[0].getTypeName()
                            + "\" and \"" + arguments[1].getTypeName() + "\"");
        }
        return returnOIResolver.get();
    }

    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object retVal = returnOIResolver.convertIfNecessary(arguments[0].get(), argOIs[0]);
        if (retVal == null) {
            retVal = returnOIResolver.convertIfNecessary(arguments[1].get(),
                    argOIs[1]);
        }
        return retVal;
    }

    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("if ");
        sb.append(children[0]);
        sb.append(" is null ");
        sb.append("returns");
        sb.append(children[1]);
        return sb.toString();
    }
}
