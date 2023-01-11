query = """
select 
    concat(pkg_name, '_', jsonb_object_keys(response -> 'versions')) as package_id,
    response -> 'dist-tags' -> 'latest' as latest_version,
    response -> 'versions' -> jsonb_object_keys(response -> 'versions') -> 'dependencies' as dependencies,
    response -> 'versions' -> jsonb_object_keys(response -> 'versions') -> 'devDependencies' as devDependencies,
    response -> 'versions' -> jsonb_object_keys(response -> 'versions') -> 'peerDependencies' as peerDependencies,
    response -> 'versions' -> jsonb_object_keys(response -> 'versions') -> 'optionalDependencies' as optionalDependencies,
    response -> 'versions' -> jsonb_object_keys(response -> 'versions') -> '_nodeVersion' as nodeVersion,
    response -> 'versions' -> jsonb_object_keys(response -> 'versions') -> '_npmVersion' as npmVersion
from analysis.tbl_pkgs_src tps
"""

query = "select pkg_name, response from analysis.tbl_pkgs_src"

import database 
import time 
start = time.time_ns()
rows = database.query(query, 'creds')
print(int((time.time_ns() - start)/ 1000000000))
print(len(rows))