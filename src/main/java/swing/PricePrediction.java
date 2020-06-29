package swing;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.LogManager;

public class PricePrediction extends JFrame {
    private JPanel title_panel;
    private JLabel Title;
    private JLabel ilLabel;
    private JLabel ilceLabel;
    private JLabel m2Label;
    private JLabel odaLabel;
    private JTextField iltextField;
    private JTextField ilcetextField;
    private JTextField m2textField;
    private JTextField odatextField;
    private JButton button;
    private JPanel mainPanel;
    private JLabel tahmin_text;
    private JComboBox comboBox1;


    //background.jpg
    public PricePrediction() throws Exception{

        Logger.getLogger("org").setLevel(Level.ERROR);
        LogManager.getLogManager().reset();
        final SparkSession spark = SparkSession.builder().appName("Prediction").master("local").getOrCreate();
        System.out.println("Spark app yüklendi");
        spark.sparkContext().setLogLevel("ERROR");
        final PipelineModel pipelineModel=PipelineModel.load("gbtRegression.modelPipelineFinal");
        System.out.println("Model yüklendi");


        comboBox1.addItem("1+1");
        comboBox1.addItem("2+1");
        comboBox1.addItem("3+1");
        comboBox1.addItem("4+1");
        comboBox1.addItem("4+2");
        comboBox1.addItem("5+1");
        comboBox1.addItem("5+2");
        comboBox1.addItem("6+1");
        comboBox1.addItem("Stüdyo");
        pack();

        //Frame
        setSize(600,400);
        setTitle("Prediction");
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        add(mainPanel);

        button.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                int m2=Integer.parseInt(m2textField.getText());
                predict(spark,m2,comboBox1.getSelectedItem().toString().toLowerCase(),
                        iltextField.getText().toLowerCase(),ilcetextField.getText().toLowerCase(),pipelineModel);
            }
        });

    }


    public void predict(SparkSession spark,int m2,String oda,String il,String ilce,PipelineModel pipelineModel){
        ArrayList<Row> list=new ArrayList<Row>();
        list.add(RowFactory.create(m2,oda.trim(),il.trim(),ilce.trim()));
        ArrayList<org.apache.spark.sql.types.StructField> listOfStructField=new ArrayList<org.apache.spark.sql.types.StructField>();
        listOfStructField.add(DataTypes.createStructField("m2", DataTypes.IntegerType, true));
        listOfStructField.add(DataTypes.createStructField("oda", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("il", DataTypes.StringType, true));
        listOfStructField.add(DataTypes.createStructField("ilce", DataTypes.StringType, true));
        StructType structType=DataTypes.createStructType(listOfStructField);
        Dataset<Row> data=spark.createDataFrame(list,structType);

        Dataset<Row> dfx=pipelineModel.transform(data).select("m2","oda","il","ilce","prediction");
        dfx.show();
        Double y = null;
        Row row=dfx.select("prediction").as(String.valueOf(y)).collectAsList().get(0);
        int d=(int)(Double.parseDouble(row.get(0).toString()));
        int x=(d/1000)*1000;
        System.out.println(x);
        tahmin_text.setText("Fiyat "+String.valueOf(x)+" TL");
    }


}
