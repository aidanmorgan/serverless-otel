rm -rf build
rm lambda.zip

mkdir -p build

cp -r *.py build
cp -r *.yaml build
pip install -r requirements-aws.txt -t build/
rm -rf build/bin # remove any binarys installed by `pip`

pushd build
zip -r lambda.zip *
popd
mv build/lambda.zip .
rm -rf build



