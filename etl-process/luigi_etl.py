
from luigi.contrib.spark import SparkSubmitTask
from luigi.contrib.hdfs import HdfsTarget
import luigi


USER = "ddevyatkin"


class PriceStat(SparkSubmitTask):
    
    app = "/home/{}/hw-etl/price_stat.py".format(USER)

    price_rosstat_path = luigi.Parameter()
    product_for_stat = luigi.Parameter()
    output_path = luigi.Parameter() 

    def output(self):
        return HdfsTarget(self.output_path)
    
    def app_command(self):
        return [
            self.app,
            self.price_rosstat_path,
            self.product_for_stat,
            self.output_path,
        ]

    
class OkDemography(SparkSubmitTask):
    
    current_dt = "2022-10-26"
    
    app = "/home/{}/hw-etl/ok_dem.py".format(USER)
    
    price_rosstat_path = luigi.Parameter()
    price_stat_path = luigi.Parameter()
    
    city_rosstat_path = luigi.Parameter()
    city_ok_rosstat_path = luigi.Parameter()
    
    demography_ok_path = luigi.Parameter()
    
    output_path = luigi.Parameter()

    def output(self):
        return HdfsTarget(self.output_path)
    
    def requires(self):
        return PriceStat()
    
    def app_command(self):
        return [
            self.app,
            self.current_dt,
            self.price_rosstat_path,
            self.price_stat_path,
            self.city_rosstat_path,
            self.city_ok_rosstat_path,
            self.demography_ok_path,
            self.output_path,
        ]
    

class ProductStat(SparkSubmitTask):
    
    app = "/home/{}/hw-etl/product_stat.py".format(USER)
    
    ok_dem_path = luigi.Parameter()

    city_rosstat_path = luigi.Parameter()
    price_rosstat_path = luigi.Parameter()

    product_rosstat_path = luigi.Parameter()
    
    output_path = luigi.Parameter()
    
    def output(self):
        return HdfsTarget(self.output_path)
    
    def requires(self):
        return OkDemography()
    
    def app_command(self):
        return [
            self.app,
            self.city_rosstat_path,
            self.price_rosstat_path,
            self.product_rosstat_path,
            self.ok_dem_path,
            self.output_path,
        ]
    
if __name__ == "__main__":
    luigi.run()