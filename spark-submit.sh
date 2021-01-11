spark-submit \
--class  org.apache.spark.BGAnalysis.Main \
--master local[4] \
--conf AWS_ACCESS_KEY="AKIAVKKVJCTAZHYRJM6I" \
--conf AWS_SECRET_KEY="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" \
C:Users\river\IdeaProjects\BoardGameAnalysis\target\scala-2.12\BoardGameAnalysis-assembly-0.1 \
100
