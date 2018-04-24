apt-get update  -q=2
apt-get install mysql-client socat -y

echo 'uuuuuuuuuuuuuuuuuuuuuuuuu'
sleep 4
echo 'uuuuuuuuuuuuuuuuuuuuuuuuu'
mysql -h database -uroot -pzxcvbnm,./  -e "use seafeventstest;"
