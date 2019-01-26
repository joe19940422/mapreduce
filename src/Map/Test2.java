package Map;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

public class Test2 {


	
	
	public  boolean isInPolygon(double pointLon, double pointLat, ArrayList<Double> polygonXA,//long
			ArrayList<Double> polygonYA) {
        // ��Ҫ�жϵĺ����������һ����
        Point2D.Double point = new Point2D.Double(pointLon, pointLat);
        // �����������ĺ�������ŵ�һ���㼯������
        List<Point2D.Double> pointList = new ArrayList<Point2D.Double>();
        double polygonPoint_x = 0.0, polygonPoint_y = 0.0;
        for (int i = 0; i < polygonXA.size(); i++) {
            polygonPoint_x = polygonXA.get(i);
            polygonPoint_y = polygonYA.get(i);
            Point2D.Double polygonPoint = new Point2D.Double(polygonPoint_x, polygonPoint_y);
            pointList.add(polygonPoint);
        }
        return check(point, pointList);
    }
 
    /**
     * һ�����Ƿ��ڶ������
     * 
     * @param point
     *            Ҫ�жϵĵ�ĺ�������
     * @param polygon
     *            ��ɵĶ������꼯��
     * @return
     */
    private  boolean check(Point2D.Double point, List<Point2D.Double> polygon) {
        java.awt.geom.GeneralPath peneralPath = new java.awt.geom.GeneralPath();
 
        Point2D.Double first = polygon.get(0);
        // ͨ���ƶ���ָ�����꣨��˫����ָ��������һ������ӵ�·����
        peneralPath.moveTo(first.x, first.y);
        polygon.remove(0);
        for (Point2D.Double d : polygon) {
            // ͨ������һ���ӵ�ǰ���굽��ָ�����꣨��˫����ָ������ֱ�ߣ���һ������ӵ�·���С�
            peneralPath.lineTo(d.x, d.y);
        }
        // �����ζ���η��
        peneralPath.lineTo(first.x, first.y);
        peneralPath.closePath();
        // ����ָ���� Point2D �Ƿ��� Shape �ı߽��ڡ�
        return peneralPath.contains(point);

    }

}
