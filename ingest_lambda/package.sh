rm -rf build
rm lambda.zip

mkdir -p build

cp -r ./*.py build
cp -r ./*.yaml build
pip install -r requirements-aws.txt -t build/
rm -rf build/bin

pushd build || exit
zip -r lambda.zip ./*
popd || exit
mv build/lambda.zip .

rm -rf build



