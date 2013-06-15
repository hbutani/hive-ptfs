package org.apache.hadoop.hive.ql.ptfs;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.exec.PTFPartition.PTFPartitionIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.PTFTranslator;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.PTFInputDef;
import org.apache.hadoop.hive.ql.plan.PTFDesc.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionResolver;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class ForecastDriver extends TableFunctionEvaluator {

  String switchover;
  ExprNodeEvaluator driverColEval;
  ExprNodeEvaluator periodColEval;
  ExprNodeEvaluator valueColEval;
  static double slope = 0;
  static double c = 0;
  static double last_historic_value = 0;
  static Map<Long,Double> pastDrivers = new LinkedHashMap<Long, Double>();
  ArrayList<String> outputColNames = new ArrayList<String>();


  @Override
  protected void execute(PTFPartitionIterator<Object> pItr, PTFPartition oPart)
      throws HiveException {
    ArrayList<Object> oRow = new ArrayList<Object>();
    double valueperdriver = 0;
    double forecastDriver = 0;
    double forecastValue = 0;

    Date switchover_date = getSwitchOverDate();

    while (pItr.hasNext())
    {
      Object iRow = pItr.next();

      DoubleWritable driverWritable = (DoubleWritable) driverColEval.evaluate(iRow);
      Double driver = driverWritable == null ? 0: driverWritable.get();

      DoubleWritable longDblWritable = (DoubleWritable) periodColEval.evaluate(iRow);
      Double longDbl = longDblWritable == null ? 0: longDblWritable.get();
      long long_date = longDbl.longValue();
      Date period = new Date(long_date*1000L);

      DoubleWritable valueWritable = (DoubleWritable) valueColEval.evaluate(iRow);
      Double value = valueWritable == null ? 0: valueWritable.get();

    //if period is in the past, define value per 1 driver. put the date value pair to the pastDrivers Map
      //Long switchover_date_ts = (switchover_date/1000L);
      if(period.before(switchover_date)){
        valueperdriver = Double.isNaN(value/driver) == true ? 0 : (value/driver);
        pastDrivers.put((period.getTime()/(1000L*60L*60L*24L)), driver);
        last_historic_value = value;
      }else{
      //if period is in the future, check if driver is filled and calculate forecastValue,
      //else, forecast driver and value
        if(driver != null){
          forecastValue = getForecastValue(driver, valueperdriver);
          value = forecastValue;
        } else{
          forecastDriver = getForecastDriver(pastDrivers, period);
          forecastValue = getForecastValue(forecastDriver, valueperdriver);
          driver = forecastDriver;
          value = forecastValue;
        }
      }

      double effect = value - last_historic_value;
      ColumnInfo effectColInfo = new ColumnInfo("effect", TypeInfoFactory.doubleTypeInfo, null, false);
      oRow.add(ObjectInspectorUtils.copyToStandardObject(new DoubleWritable(effect), effectColInfo.getObjectInspector()));

      StructObjectInspector inputOI = getTableDef().getInput().getOutputShape().getOI();
      List<? extends StructField> inputFields = inputOI.getAllStructFieldRefs();
      for (StructField structField : inputFields) {
        oRow.add(ObjectInspectorUtils.copyToStandardObject(inputOI.getStructFieldData(iRow, structField),
            structField.getFieldObjectInspector()));
      }
      oPart.append(oRow);
    }

  }

  protected double getForecastValue(double driver, double valueperdriver) {
    return driver * valueperdriver;
  }

  protected Double getForecastDriver(Map<Long,Double> pastDrivers, Date period){
    //use linear regression to determine the value for the given period
    // first pass: compute xbar and ybar
    if(slope == 0){
        double sumx = 0.0, sumy = 0.0, sumx2 = 0.0;

        Iterator<Long> pastDriverItr1 = pastDrivers.keySet().iterator();
        while (pastDriverItr1.hasNext()) {
          long periodTime = pastDriverItr1.next();
          double driver = pastDrivers.get(periodTime);
          sumy += driver;
          sumx += periodTime;
          sumx2 += periodTime * periodTime;
        }

        double xbar = sumx / pastDrivers.size();
        double ybar = sumy / pastDrivers.size();
        // second pass: compute summary statistics
        double xxbar = 0.0, xybar = 0.0;

        Iterator<Long> pastDriverItr2 = pastDrivers.keySet().iterator();
        while (pastDriverItr2.hasNext()) {
          long periodTime = pastDriverItr2.next();
          double driver = pastDrivers.get(periodTime);
          xxbar += (periodTime - xbar) * (periodTime - xbar);
          xybar += (periodTime  - xbar) * (driver - xbar);
        }

        slope = xybar / xxbar;
        c = ybar - slope * xbar;

        // slope and c are interpreted like this: "y   =  slope*x + c);
    }

    if(pastDrivers.size() == 1){
      Iterator<Long> periodtimesItr = pastDrivers.keySet().iterator();
      slope = 0;
      c = pastDrivers.get(periodtimesItr.next());
    }
    return (((period.getTime()/(1000L*60L*60L*24L))*slope) + c);

  }



  protected Date getSwitchOverDate(){
    //if switchover is empty, return max date
    if(switchover == null){
      Date maxdate = new Date(Long.MAX_VALUE);
      return(maxdate);
    }

    if(switchover.equalsIgnoreCase("today")){
      //if switchover is today, return todays date
      Date switchover_date = new Date();
      return(switchover_date);
    }
    //check if switchover is valid date and return
    @SuppressWarnings("deprecation")
    Date switchover_date = new Date(
        ((Integer.parseInt(switchover.substring(0, 4))-1900)),
        ((Integer.parseInt(switchover.substring(5, 7)))-1),
        (Integer.parseInt(switchover.substring(8, 10))));
    return switchover_date;
  }

  public static class ForecastDriverResolver extends TableFunctionResolver
  {
    public static final String EFFECT_COL = "effect";

    @Override
    public void setupOutputOI() throws SemanticException {
      ForecastDriver evaluator = (ForecastDriver) getEvaluator();
      PartitionedTableFunctionDef tDef = evaluator.getTableDef();
      initializeEvaluator(evaluator, tDef);

      RowResolver rr = new RowResolver();
      ColumnInfo effectColInfo = new ColumnInfo(EFFECT_COL, TypeInfoFactory.doubleTypeInfo, null, false);
      rr.put(null, EFFECT_COL, effectColInfo);
      evaluator.outputColNames.add(EFFECT_COL);

      setOutputOI((StructObjectInspector) createOutputOI(tDef.getInput(), rr, evaluator));
    }

    private void initializeEvaluator(ForecastDriver evaluator, PartitionedTableFunctionDef tDef) throws SemanticException{
      ArrayList<PTFExpressionDef> args = tDef.getArgs();
      int argsNum = args == null ? 0 : args.size();

      if ( argsNum < 4 )
      {
        throw new SemanticException("at least 4 arguments required");
      }

      ObjectInspector switchoverOI = args.get(0).getOI();
      if(isPrimitivestring(switchoverOI)){
        throw new SemanticException("Support only String constant values for switchOver(1st arg).");
      }

      evaluator.switchover = ((ConstantObjectInspector)switchoverOI).getWritableConstantValue().toString();
      evaluator.driverColEval = args.get(1).getExprEvaluator();
      evaluator.periodColEval = args.get(2).getExprEvaluator();
      evaluator.valueColEval = args.get(3).getExprEvaluator();
    }



    private boolean isPrimitivestring(ObjectInspector primeColOI){
      return !ObjectInspectorUtils.isConstantObjectInspector(primeColOI) ||
          (primeColOI.getCategory() != ObjectInspector.Category.PRIMITIVE) ||
          ((PrimitiveObjectInspector)primeColOI).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING ;
    }

    protected static ObjectInspector createOutputOI(PTFInputDef inpDef, RowResolver rr , ForecastDriver evaluator) throws SemanticException {
      RowResolver inputRR = inpDef.getOutputShape().getRr();
      for (ColumnInfo inpCInfo : inputRR.getColumnInfos()) {
        ColumnInfo cInfo = new ColumnInfo(inpCInfo);
        String colAlias = cInfo.getAlias();
        String[] tabColAlias = inputRR.reverseLookup(inpCInfo.getInternalName());
        if (tabColAlias != null) {
          colAlias = tabColAlias[1];
        }
        ASTNode inExpr = null;
        inExpr = PTFTranslator.getASTNode(inpCInfo, inputRR);
        if ( inExpr != null ) {
          rr.putExpression(inExpr, cInfo);
        }
        else {
          rr.put(cInfo.getTabAlias(), colAlias, cInfo);
        }
        evaluator.outputColNames.add(colAlias);
      }

      return PTFTranslator.getStandardStructOI(rr);
    }



    @Override
    public void initializeOutputOI() throws HiveException {
      ForecastDriver evaluator = (ForecastDriver) getEvaluator();
      PartitionedTableFunctionDef tDef = evaluator.getTableDef();
      initializeEvaluator(evaluator, tDef);

      StructObjectInspector OI = tDef.getOutputShape().getOI();

      ArrayList<String> colNames = new ArrayList<String>();
      List<ObjectInspector> colOIs = new ArrayList<ObjectInspector>();
      for (StructField field : OI.getAllStructFieldRefs()) {
        String name = field.getFieldName();
        evaluator.outputColNames.add(name);
        colNames.add(name);
        colOIs.add(field.getFieldObjectInspector());
      }

      StructObjectInspector stdOI = ObjectInspectorFactory.getStandardStructObjectInspector(
          colNames, colOIs);

      setOutputOI(stdOI);
    }

    @Override
    public boolean transformsRawInput() {
      return false;
    }

    @Override
    public ArrayList<String> getOutputColumnNames() throws SemanticException {
      ForecastDriver evaluator = (ForecastDriver) getEvaluator();
      return evaluator.outputColNames;
    }

    @Override
    protected TableFunctionEvaluator createEvaluator(PTFDesc ptfDesc,
        PartitionedTableFunctionDef tDef) {
      return new ForecastDriver();
    }

  }


}