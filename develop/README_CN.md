## Quick Start

```bash
./develop/scripts/install.sh # or your relative/full path
```


## test


```bash
docker exec -it pixels_postgres_source_db mysql -upixels -ppixels_realtime_crud -D pixels_realtime_crud

docker exec -it pixels_postgres_source_db psql -Upixels -d pixels_realtime_crud
```