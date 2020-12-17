clean:
	@echo "Removing dist directory"
	rm -rf dist/

test: clean
	@echo "Building project"
	python setup.py build bdist_wheel
	@echo "Uploading wheel file to S3"
	aws s3 cp dist/easyglue-*.whl s3://bertolb/test/pylibs/
