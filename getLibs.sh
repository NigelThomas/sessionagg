#!/bin/bash
# $Id: //depot/insight/ITS/apps/ttdsapi/libs/getTtdsLibs.sh#9 $
# Copyright (C) 2014-2015 SQLstream, Inc.
# Last Updated:    $Date: 2018/07/12 $
# Last Updated By: $Author: nigel $

HERE=$(cd `dirname $0`; pwd)
cd $HERE/lib

# link to a directory somewhere on this server
function linkto () {
	ld=$1
	f=$2

	echo -n ... $f

	# always remove and restore the link, in case SQLstream version has changed

	if [ -e $f -o -L $f ]
	then
		echo -n ... remove existing file or link
		rm $f
    fi

    if [ -r $ld/$f ]
    then
        ln -s $ld/$f .
        echo -n ... linked
    else
        echo -n ... WARNING not found in $ld
    fi
    echo
}

# copy jar from somewhere local
function copyFrom () {
	ld=$1
	f=$2

	echo -n ... $f 

	if [ -e $f ]
	then
		echo ... file already present
	else
		if [ -r $ld/$f ]
		then
			cp $ld/$f .
			echo ... copied
		else
			echo ... WARNING not found in $ld
		fi
	fi
}

# get a jar from a url - the url ends with the jar name
function getJar {
	url=$1
	jar=`basename $url`
	getJarFromRemote $jar $url
}


# get a jar from a url where the jar name is different from the last component of the url
function getJarFromRemote () {
	jar=$1
	url=$2

	echo -n ... $jar

	if [ -e $jar ]
	then
		echo ... present
	else
		wget -O $jar $url
		echo ... got from $url
	fi
}

# get a jar from a zip file, supplying the jar, zip, zip subdirectory and url
function getJarFromRemoteZip() {
	jar=$1
	zip=$2
	zipdir=$3
	url=$4

	echo -n ... $jar

	if [ -e $jar ]
	then
		echo ... present
	else
		if [ ! -r $zip ]
		then
			wget -O $zip $url 
			echo -n ... got from $url
		fi
		if [ "$zipdir" == "." -o -z "$zipdir" ]
		then
			unzip -d . -j $zip  $jar
		else
			unzip -d . -j $zip  $zipdir/$jar
		fi
		echo ... unzipped
		# leave zip file in place in case user wants to use javadoc or other content	
	fi
}

# get a jar from a remote tar (or tar.gz) specifying jar name, tar name, sub directory and url
function getJarFromRemoteTar () {
	jar=$1
	tar=$2
	sd=$3
	url=$4

	echo -n ... $jar

	if [ -e $jar ]
	then
		echo ... present
	else
		if [ ! -e $tar ]
		then
			wget $url
			echo -n ... got from $url
		fi
		tar xvzf $tar $sd/$jar
		if [ "$sd" != "." ]
		then
			mv $sd/$jar .
			# now remove the extract directories, from the bottom up
			x=$sd
			while [ "$x" != "." ]
			do
				rmdir $x
				x=`dirname $x`
			done
		fi
		echo ... un-tarred
	fi
}

HERE=$(cd `dirname $0`; pwd)
ITS_HOME=$(cd $HERE/../../.. ; pwd)
cd $HERE


for f in  aspen-core.jar sqlstream-jdbc-complete.jar 
do
	linkto $SQLSTREAM_HOME/lib $f
done


cd -
