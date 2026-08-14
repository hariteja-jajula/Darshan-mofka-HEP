# START CREATE TOPIC
# long arguments version
diaspora-ctl topic create --name collisions \
    --driver files --driver.root_path /tmp/my-data \
    --topic.num_partitions 3 \
    --validator energy_validator:libenergy_validator.so \
    --validator.energy_max 100 \
    --partition-selector energy_partition_selector:libenergy_partition_selector.so \
    --partition-selector.energy_max 100 \
    --serializer energy_serializer:libenergy_serializer.so \
    --serializer.energy_max 100
# END CREATE TOPIC
