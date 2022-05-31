#!/usr/bin/env bash

databusclient deploy \
	--versionid "https://d8lr.tools.dbpedia.org/hopver/testGroup/testArtifact/1.0-alpha/" \
	--title "Test Title" \
	--abstract "Test Abstract" \
	--description "Test Description" \
	--license "http://dalicc.net/licenselibrary/AdaptivePublicLicense10" \
	--apikey "$1" \
	"https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml|type=swagger"
