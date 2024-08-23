
cd /home/cxa/nfs-ganesha-v0
rm src/cmake-build-debug/* -rf


cmake -S src -B src/cmake-build-debug -DCMAKE_BUILD_TYPE=Debug \
-DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
-DUSE_FSAL_VFS=ON -DUSE_9P=OFF \
-DUSE_FSAL_LUSTRE=OFF -DUSE_FSAL_GPFS=OFF \
-DUSE_NLM=OFF -DUSE_FSAL_CEPH:STRING=OFF \
-DUSE_FSAL_GLUSTER:STRING=OFF -DUSE_FSAL_KVSFS:STRING=OFF \
-DUSE_FSAL_LIZARDFS:STRING=OFF -DUSE_FSAL_NULL:STRING=OFF \
-DUSE_FSAL_MEM:STRING=OFF -DUSE_FSAL_PROXY_V3:STRING=OFF \
-DUSE_FSAL_PROXY_V4:STRING=OFF -DUSE_FSAL_RGW:STRING=OFF \
-DUSE_FSAL_XFS:STRING=OFF -DUSE_GSS=OFF

cmake --build src/cmake-build-debug --parallel 32

cp src/cmake-build-debug/compile_commands.json ./

# delete rocksdb
echo "start to delete rocksdb"
sudo rm /home/cxa/test_rocksdb/* -rf
# sudo chown -R cxa:cxa /home/cxa/test_rocksdb
# g++ -std=c++17 -o clear_rocksdb clear_rocksdb.cpp -lrocksdb -lz -lbz2 -lsnappy -llz4 -lzstd -pthread
# ./clear_rocksdb

# clear logs
echo "start to clear log"
echo "" > /var/log/nfs-ganesha.log
# echo "" > /var/log/mysqld.log
# echo "" > /var/log/mysql_query.log

# clear nfs-server
echo "start to reset nfs_server dir"
rm ~/nfs_server/* -rf
## rsync -avz --partial --progress n2:/home/cxa/init_dir/* /home/cxa/nfs_server/
cp ~/nfs_test/* ~/nfs_server/ -r
# cp /home/cxa/mysql/init_dir/* ~/nfs_server/ -r
# chmod -R 777 ~/nfs_server/data

path="/var/run/ganesha"
# 使用 if 语句检查目录是否存在
if [ ! -d "$path" ]; then
    echo "$path does not exist. Create it."
    sudo mkdir -p "$path"
    sudo /etc/init.d/nfs-kernel-server stop
fi
