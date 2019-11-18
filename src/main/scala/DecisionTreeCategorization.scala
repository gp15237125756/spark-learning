import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.regression.DecisionTreeRegressor

case class Iris(features: org.apache.spark.ml.linalg.Vector, label: String)

object DecisionTreeCategorization {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkWriteHive").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile("file:///usr/local/spark/iris.txt").map(_.split(",")).map(p => Iris(Vectors.dense(p(0).toDouble,p(1).toDouble,p(2).toDouble, p(3).toDouble),p(4).toString())).toDF()
    data.createOrReplaceTempView("iris")
    val df = spark.sql("select * from iris")
    df.map(t => t(1)+":"+t(0)).collect().foreach(println)
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol(
      "indexedLabel").fit(df)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutpu
    tCol("indexedFeatures").setMaxCategories(4).fit(df)
    val labelConverter = new IndexToString().setInputCol("prediction").setOut
    putCol("predictedLabel").setLabels(labelIndexer.labels)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    val dtClassifier = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")
    val pipelinedClassifier = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dtClassifier, labelConverter))
    val modelClassifier = pipelinedClassifier.fit(trainingData)
    val predictionsClassifier = modelClassifier.transform(testData)
    predictionsClassifier.select("predictedLabel", "label", "features").show(20)
    val evaluatorClassifier = new MulticlassClassificationEvaluator().s
    etLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
    val accuracy = evaluatorClassifier.evaluate(predictionsClassifier)
    println("Test Error = " + (1.0 - accuracy))
    val treeModelClassifier = modelClassifier.stages(2).asInstanceOf[De
      cisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModelClassifier.toDebugString)
    val dtRegressor = new DecisionTreeRegressor().setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
    val pipelineRegressor = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dtRegressor, labelConverter))
    val modelRegressor = pipelineRegressor.fit(trainingData)
    val predictionsRegressor = modelRegressor.transform(testData)
    predictionsRegressor.select("predictedLabel", "label", "features").show(20)
    val evaluatorRegressor = new RegressionEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("rmse")
      val rmse = evaluatorRegressor.evaluate(predictionsRegressor)
      println("Root Mean Squared Error (RMSE) on test data = " + rmse)
    val treeModelRegressor = modelRegressor.stages(2).asInstanceOf[DecisionTreeRegressionModel]
    println("Learned regression tree model:\n" + treeModelRegressor.toDebugString)


  }

}
