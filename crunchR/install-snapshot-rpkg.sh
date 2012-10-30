# compile AND install R package. 
# need to have R, rJava  and have a sudo 
# for this to work. Also have maven executable around

MVN='mvn clean install -DskipTests -DR'
ver=`mvn help:evaluate -Dexpression=project.version | grep -vi "\\[INFO\\]"`

if [[ "$ver" == "" ]]; then 
  echo unable to deduce snapshot version.
  exit 1
fi


R CMD REMOVE crunchR; { $MVN && R_COMPILE_PKGS=1 R CMD INSTALL --build target/crunchR-${ver}-rpkg; }
