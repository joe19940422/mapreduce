package Map;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.List;

public class Test2 {


	
	
	public  boolean isInPolygon(double pointLon, double pointLat, ArrayList<Double> polygonXA,//long
			ArrayList<Double> polygonYA) {
        // 将要判断的横纵坐标组成一个点
        Point2D.Double point = new Point2D.Double(pointLon, pointLat);
        // 将区域各顶点的横纵坐标放到一个点集合里面
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
     * 一个点是否在多边形内
     * 
     * @param point
     *            要判断的点的横纵坐标
     * @param polygon
     *            组成的顶点坐标集合
     * @return
     */
    private  boolean check(Point2D.Double point, List<Point2D.Double> polygon) {
        java.awt.geom.GeneralPath peneralPath = new java.awt.geom.GeneralPath();
 
        Point2D.Double first = polygon.get(0);
        // 通过移动到指定坐标（以双精度指定），将一个点添加到路径中
        peneralPath.moveTo(first.x, first.y);
        polygon.remove(0);
        for (Point2D.Double d : polygon) {
            // 通过绘制一条从当前坐标到新指定坐标（以双精度指定）的直线，将一个点添加到路径中。
            peneralPath.lineTo(d.x, d.y);
        }
        // 将几何多边形封闭
        peneralPath.lineTo(first.x, first.y);
        peneralPath.closePath();
        // 测试指定的 Point2D 是否在 Shape 的边界内。
        return peneralPath.contains(point);

    }

}
