#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <boost/geometry/index/rtree.hpp>

#include <cmath>
#include <vector>
#include <iostream>
#include<fstream>
#include <boost/foreach.hpp>
#include <sys/time.h>
namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

int main()
{
    typedef bg::model::point<float, 2, bg::cs::cartesian> point;
    typedef bg::model::box<point> box;
    typedef bg::model::polygon<point, false, false> polygon; // ccw, open polygon
    typedef std::pair<box, unsigned> value;

    // polygons
    std::vector<polygon> polygons;
	polygon p;
    
    std::ifstream file;
	file.open("C:/Users/sidharth/Downloads/polygon.txt");
    std::string str; 
    int i = 0;
    while (std::getline(file, str))
    {
    	boost::geometry::read_wkt(str, p);
		
    	 polygons.push_back(p);
        
    }
   

    // create the rtree using default constructor
    bgi::rtree< value, bgi::rstar<16, 4> > rtree;

    // fill the spatial index
    for ( unsigned i = 0 ; i < polygons.size() ; ++i )
    {
        // calculate polygon bounding box
        box b = bg::return_envelope<box>(polygons[i]);
        // insert new value
        rtree.insert(std::make_pair(b, i));
    }

    // find 5 nearest values to a point
    std::vector<value> result_n;
    for (int i =0; i<5; i++) {
    rtree.query(bgi::nearest(point(i, i), 5), std::back_inserter(result_n));
	}
    
    std::cout << "knn query point:" << std::endl;
    std::cout << bg::wkt<point>(point(0, 0)) << std::endl;
    std::cout << "knn query result:" << std::endl;
    BOOST_FOREACH(value const& v, result_n)
        std::cout << bg::wkt<polygon>(polygons[v.second]) << std::endl;
    return 0;
}
