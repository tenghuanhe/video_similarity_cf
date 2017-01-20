import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by tenghuanhe on 2017/1/19.
  */
object VideoSimilarityDistributed {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Video Similarity CF").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val user_video_pref_rdd = sc.textFile("user_video_pref.txt").map {
      case line =>
        val tuple = line.split(" ")
        val user_id = tuple(0).toInt
        val video_id = tuple(1).toInt
        val pref = tuple(2).toDouble
        (user_id, video_id, pref)
    }

    // groupBy user_id
    // => List(user_id, List((video_id, pref)))
    val video_list_by_user_rdd = user_video_pref_rdd.groupBy(x => x._1).map {
      case (user_id, video_pref_list) =>
        (user_id, video_pref_list.map(x => (x._2, x._3)))
    }

    // flatten
    // => List(v1id, v2id, v1_pref, v2_pref, user_id)
    val video_pref_pairwise_user_rdd = video_list_by_user_rdd.map {
      case (user_id, video_pref_list) =>
        val list = for (v1 <- video_pref_list; v2 <- video_pref_list) yield (v1, v2)
        list.filter(x => x._1._1 != x._2._1).map(x => (x._1._1, x._2._1, x._1._2, x._2._2, user_id))
    }.flatMap(list => list)

    // groupBy (v1id, v2id)
    // => List((v1id, v2id), List((v1_pref, v2_pref, user_id)))
    val v1v2_rdd = video_pref_pairwise_user_rdd.groupBy(x => (x._1, x._2)).map {
      case ((v1id, v2id), v1v2p1p2_user_id_list) =>
        (v1id, v2id, v1v2p1p2_user_id_list.map(x => (x._3, x._4, x._5)))
    }

    // calculate similarity
    // => List((v1id, v2id, similarity))
    val similarity_rdd = v1v2_rdd.map {
      case (v1id, v2id, p1p2_uid_list) =>
        val norm_v1 = p1p2_uid_list.map(x => x._1 * x._1).sum
        val norm_v2 = p1p2_uid_list.map(x => x._2 * x._2).sum
        val v1dotv2 = p1p2_uid_list.map(x => x._1 * x._2).sum
        val similarity = v1dotv2 / (Math.sqrt(norm_v1) * Math.sqrt(norm_v2))
        (v1id, v2id, similarity)
    }

    //  gropby v1id and got top 10 similar videos for each video
    // => List((vid, List((vid, similarity)))
    val similarity_rdd_top10 = similarity_rdd.groupBy(x => x._1).map {
      case (vid, similarity_list) =>
        val list = similarity_list.map(x => (x._2, x._3)).toList.sortBy(_._2).reverse.take(10)
          .map(x => x._1 + ":" + x._2).mkString("\t")
        vid + "\t" + list
    }

    similarity_rdd_top10.saveAsTextFile("video_similarity_top10_spark_distributed")
  }
}