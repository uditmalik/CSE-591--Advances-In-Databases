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
namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;
#include <windows.h>
#include <stdio.h>
#include <psapi.h>

// To ensure correct resolution of symbols, add Psapi.lib to TARGETLIBS
// and compile with -DPSAPI_VERSION=1

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

    // find values intersecting some area defined by a box
    std::vector<value> result_s;
    for (int i =0; i<1; i++) {
	
    box query_box(point(962269+i , 173705+i), point(997902+i, 234692+i));
   
    rtree.query(bgi::within(query_box), std::back_inserter(result_s));
	}
	
    // display results
    std::cout << "range query result:" << std::endl;
    BOOST_FOREACH(value const& v, result_s)
        std::cout << bg::wkt<polygon>(polygons[v.second]) << std::endl;   
     return 0;
}
