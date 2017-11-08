import { DistrictSpider } from './gaode/DistrictSpider';
import { PoiSpider } from './gaode/PoiSpider';
import { RoadSpider } from "./gaode/RoadSpider";

// new PoiSpider().init();
// new RoadSpider().init();
new DistrictSpider().start();