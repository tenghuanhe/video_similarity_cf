import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.output.StringBuilderWriter;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by tenghuanhe on 2017/1/19.
 */

@SuppressWarnings("unchecked")
public class VideoSimilaritySingle {
  public static void main(String[] args) throws IOException {

    InputStream fis = new FileInputStream("user_video_pref.txt");
    InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
    BufferedReader br = new BufferedReader(isr);

    Map<Integer, List> userIdVideoListMap = new HashedMap();
    String line;
    while ((line = br.readLine()) != null) {
      String[] tuple = line.split(" ");
      Integer user_id = Integer.valueOf(tuple[0]);
      Integer video_id = Integer.valueOf(tuple[1]);
      double pref = Double.parseDouble(tuple[2]);
      if (userIdVideoListMap.get(user_id) == null) {
        List<Tuple2<Integer, Double>> videoPrefTupleList = new ArrayList();
        videoPrefTupleList.add(new Tuple2<>(video_id, pref));
        userIdVideoListMap.put(user_id, videoPrefTupleList);
      } else {
        userIdVideoListMap.get(user_id).add(new Tuple2(video_id, pref));
      }
    }

    br.close();
    isr.close();
    fis.close();

    List<Tuple5<Integer, Integer, Double, Double, Integer>> userIdVideoPrefPairList = new ArrayList();

    for (Map.Entry<Integer, List> entry : userIdVideoListMap.entrySet()) {
      Integer userId = entry.getKey();
      List videoPrefPairList = entry.getValue();

      for (int i = 0; i < videoPrefPairList.size(); i++) {
        for (int j = 0; j < videoPrefPairList.size(); j++) {
          Tuple2<Integer, Double> videoPref1 = (Tuple2) videoPrefPairList.get(i);
          Tuple2<Integer, Double> videoPref2 = (Tuple2) videoPrefPairList.get(j);
          if (!videoPref1._1.equals(videoPref2._1)) {
            userIdVideoPrefPairList.add(new Tuple5<>(videoPref1._1, videoPref2._1, videoPref1._2, videoPref2._2, userId));
          }
        }
      }
    }

    Map<Tuple2<Integer, Integer>, List<Tuple3<Double, Double, Integer>>> videoPairUserIdPrefMap = new HashMap<>();

    for (Tuple5<Integer, Integer, Double, Double, Integer> tuple5 :
        userIdVideoPrefPairList) {
      Tuple2<Integer, Integer> videoIdPair = new Tuple2(tuple5._1(), tuple5._2());
      Tuple3<Double, Double, Integer> prefPairUserId = new Tuple3(tuple5._3(), tuple5._4(), tuple5._5());
      if (videoPairUserIdPrefMap.get(videoIdPair) == null) {
        List<Tuple3<Double, Double, Integer>> userIdPrefList = new ArrayList<>();
        userIdPrefList.add(prefPairUserId);
        videoPairUserIdPrefMap.put(videoIdPair, userIdPrefList);
      } else {
        videoPairUserIdPrefMap.get(videoIdPair).add(prefPairUserId);
      }
    }

    List<Tuple3<Integer, Integer, Double>> videoPairSimilarity = new ArrayList<>();
    for (Map.Entry<Tuple2<Integer, Integer>, List<Tuple3<Double, Double, Integer>>> entry : videoPairUserIdPrefMap.entrySet()) {
      Tuple2<Integer, Integer> videoIdPair = entry.getKey();
      List<Tuple3<Double, Double, Integer>> prefPairUserIdList = entry.getValue();

      double normV1 = 0;
      double normV2 = 0;
      double v1DotV2 = 0;

      for (Tuple3<Double, Double, Integer> prefPairUserId :
          prefPairUserIdList) {
        normV1 += prefPairUserId._1() * prefPairUserId._1();
        normV2 += prefPairUserId._2() * prefPairUserId._2();
        v1DotV2 += prefPairUserId._1() * prefPairUserId._2();
      }

      double similarity = v1DotV2 / (Math.sqrt(normV1) * Math.sqrt(normV2));
      videoPairSimilarity.add(new Tuple3<>(videoIdPair._1, videoIdPair._2, similarity));
    }

    Map<Integer, List<Tuple2<Integer, Double>>> videoIdSimilarVideoMap = new HashMap<>();
    for (Tuple3<Integer, Integer, Double> videoPairSim : videoPairSimilarity) {
      Integer v1id = videoPairSim._1();
      Integer v2id = videoPairSim._2();
      Double similarity = videoPairSim._3();
      if (videoIdSimilarVideoMap.get(v1id) == null) {
        List<Tuple2<Integer, Double>> list = new ArrayList<>();
        list.add(new Tuple2<>(v2id, similarity));
        videoIdSimilarVideoMap.put(v1id, list);
      } else {
        videoIdSimilarVideoMap.get(v1id).add(new Tuple2<>(v2id, similarity));
      }
    }

    // sort and output

    OutputStream fos = new FileOutputStream("video_similarity_top10_java_single_node.txt");
    OutputStreamWriter osw = new OutputStreamWriter(fos, Charset.forName("UTF-8"));
    BufferedWriter bw = new BufferedWriter(osw);

    for (Map.Entry<Integer, List<Tuple2<Integer, Double>>> entry : videoIdSimilarVideoMap.entrySet()
        ) {

      StringBuilder sb = new StringBuilder();
      Integer vid = entry.getKey();
      List<Tuple2<Integer, Double>> vSimList = entry.getValue();

      sb.append(vid);

      vSimList.sort((o1, o2) -> {
        if (o1._2 < o2._2) {
          return 1;
        } else if (o1._2 > o2._2) {
          return -1;
        } else {
          return 0;
        }
      });

      for (Tuple2<Integer, Double> vs : vSimList.subList(0, 10)) {
        sb.append("\t" + vs._1 + ":" + vs._2);
      }

      sb.append("\n");

      bw.write(sb.toString());
    }

    bw.close();
    osw.close();
    fos.close();
  }
}

