clean:
	@echo "Cleaning build directories"
	rm -rf dist/
	rm -rf build/
	rm -rf easyglue.egg-info/

test: clean
	@echo "Building project"
	python3 setup.py build bdist_wheel
	@echo "Uploading wheel file to S3"
	aws s3 cp dist/easyglue-*.whl s3://bertolb/test/pylibs/
