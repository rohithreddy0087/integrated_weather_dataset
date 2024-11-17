#!/bin/bash


WGET_VERSION=$(wget --version | grep -oie "wget [0-9][0-9.]*" | head -n 1 | awk '{print $2}')
if [ -z "$WGET_VERSION" ]
then
WGET_VERSION=PARSE_ERROR
fi

WGET_USER_AGENT="wget/$WGET_VERSION/gateway/4.4.10-20240516-151433"


##############################################################################
#
# Generated by: NCAR Climate Data Gateway
# Created: 2024-05-28T15:28:39-06:00
#
# Template version: 0.4.7-wget-checksum
#
#
# Your download selection includes data that might be secured using API Token based
# authentication. Therefore, this script can have your api-token. If you
# re-generate your API Token after you download this script, the download will
# fail. If that happens, you can either re-download the script or you can edit
# this script replacing the old API Token with the new one. View your API token
# by going to "Account Home":
#
# https://www.earthsystemgrid.org/account/user/account-home.html
#
# and clicking on "API Token" link under "Personal Account". You will be asked
# to log into the application before you can view your API Token.
#
#
# Dataset
# ucar.cgd.artmip.tier1.catalogues.rutz
# ba94d53e-6d90-4a9b-a7a8-bdf2d7313737
# https://www.earthsystemgrid.org/dataset/ucar.cgd.artmip.tier1.catalogues.rutz.html
# https://www.earthsystemgrid.org/dataset/id/ba94d53e-6d90-4a9b-a7a8-bdf2d7313737.html
#
# Dataset Version
# 1
# b8a9b50a-cdfd-43f9-a65f-2110ec6d5a36
# https://www.earthsystemgrid.org/dataset/ucar.cgd.artmip.tier1.catalogues.rutz/version/1.html
# https://www.earthsystemgrid.org/dataset/version/id/b8a9b50a-cdfd-43f9-a65f-2110ec6d5a36.html
#
##############################################################################

CACHE_FILE=.md5_results
MAX_RETRY=3


usage() {
    echo "Usage: $(basename $0) [flags]"
    echo "Flags is one of:"
    sed -n '/^while getopts/,/^done/  s/^\([^)]*\)[^#]*#\(.*$\)/\1 \2/p' $0
}
#defaults
debug=0
clean_work=1
verbose=1

#parse flags

while getopts ':pdvqko:' OPT; do

    case $OPT in

        p) clean_work=0;;       #	: preserve data that failed checksum
        o) output="$OPTARG";;   #<file>	: Write output for DML in the given file
        d) debug=1;;            #	: display debug information
        v) verbose=1;;          #       : be more verbose
        q) quiet=1;;            #	: be less verbose
        k) cert=1;;            #	: add --no-check-certificate
        \?) echo "Unknown option '$OPTARG'" >&2 && usage && exit 1;;
        \:) echo "Missing parameter for flag '$OPTARG'" >&2 && usage && exit 1;;
    esac
done
shift $(($OPTIND - 1))

if [[ "$output" ]]; then
    #check and prepare the file
    if [[ -f "$output" ]]; then
        read -p "Overwrite existing file $output? (y/N) " answ
        case $answ in y|Y|yes|Yes);; *) echo "Aborting then..."; exit 0;; esac
    fi
    : > "$output" || { echo "Can't write file $output"; break; }
fi

    ((debug)) && echo "debug=$debug, cert=$cert, verbose=$verbose, quiet=$quiet, clean_work=$clean_work"

##############################################################################


check_chksum() {
    local file="$1"
    local chk_type=$2
    local chk_value=$3
    local local_chksum

    case $chk_type in
        md5) local_chksum=$(md5sum "$file" | cut -f1 -d" ");;
        *) echo "Can't verify checksum." && return 0;;
    esac

    #verify
    ((debug)) && echo "local:$local_chksum vs remote:$chk_value"
    diff -q <(echo $local_chksum) <(echo $chk_value) >/dev/null
}

download() {

    if [[ "$cert" ]]; then
      wget="wget --no-check-certificate -c --user-agent=$WGET_USER_AGENT"
    else
      wget="wget -c --user-agent=$WGET_USER_AGENT"
    fi

    ((quiet)) && wget="$wget -q" || { ((!verbose)) && wget="$wget -nv"; }

    ((debug)) && echo "wget command: $wget"

    while read line
    do
        # read csv here document into proper variables
        eval $(awk -F "' '" '{$0=substr($0,2,length($0)-2); $3=tolower($3); print "file=\""$1"\";url=\""$2"\";chksum_type=\""$3"\";chksum=\""$4"\""}' <(echo $line) )

        #Process the file
        echo -n "$file ..."

        #are we just writing a file?
        if [ "$output" ]; then
            echo "$file - $url" >> $output
            echo ""
            continue
        fi

        retry_counter=0

        while : ; do
                #if we have the file, check if it's already processed.
                [ -f "$file" ] && cached="$(grep $file $CACHE_FILE)" || unset cached

                #check it wasn't modified
                if [[ -n "$cached" && "$(stat -c %Y $file)" == $(echo "$cached" | cut -d ' ' -f2) ]]; then
                    echo "Already downloaded and verified"
                    break
                fi

                # (if we had the file size, we could check before trying to complete)
                echo "Downloading"
                $wget -O "$file" $url || { failed=1; break; }

                #check if file is there
                if [[ -f "$file" ]]; then
                        ((debug)) && echo file found
                        if ! check_chksum "$file" $chksum_type $chksum; then
                                echo "  $chksum_type failed!"
                                if ((clean_work)); then
                                        rm "$file"

                                        #try again up to n times
                                        echo -n "  Re-downloading..."

                                        if [ $retry_counter -eq $MAX_RETRY]
                                        then
                                            echo "  Re-tried file $file $MAX_RETRY times...."
                                            break
                                        fi
                                        retry_counter=`expr $retry_counter + 1`

                                        continue
                                else
                                        echo "  don't use -p or remove manually."
                                fi
                        else
                                echo "  $chksum_type ok. done!"
                                echo "$file" $(stat -c %Y "$file") $chksum >> $CACHE_FILE
                        fi
                fi
                #done!
                break
        done

        if ((failed)); then
            echo "download failed"

            unset failed
        fi

    done <<EOF--dataset.file.url.chksum_type.chksum
'MERRA2.ar_tag.Rutz.3hourly.20000101-20001231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20000101-20001231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '3620f7d1491af41ba2143d91e5dbfbff'
'MERRA2.ar_tag.Rutz.3hourly.20010101-20011231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20010101-20011231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'f9f843087b88203bf81c8ad29b2c5913'
'MERRA2.ar_tag.Rutz.3hourly.20020101-20021231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20020101-20021231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '62c4da6a66697d16a860cf2360daf6f4'
'MERRA2.ar_tag.Rutz.3hourly.20030101-20031231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20030101-20031231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'cedba3128926d542e357856eabaa358a'
'MERRA2.ar_tag.Rutz.3hourly.20040101-20041231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20040101-20041231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '7d9aa074c103c580f8b5e6ee0d6dc1b3'
'MERRA2.ar_tag.Rutz.3hourly.20050101-20051231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20050101-20051231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '0799e5c6dcd2585bab7209523bab36a2'
'MERRA2.ar_tag.Rutz.3hourly.20060101-20061231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20060101-20061231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'fd19a9c1137864467fd928d009fa319f'
'MERRA2.ar_tag.Rutz.3hourly.20070101-20071231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20070101-20071231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'e113246262bded41382899b7479dc2a9'
'MERRA2.ar_tag.Rutz.3hourly.20080101-20081231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20080101-20081231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '851440299eaa1ac9e3605af4acaead79'
'MERRA2.ar_tag.Rutz.3hourly.20090101-20091231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20090101-20091231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '1d639a45ac436765316baceb0a7244e1'
'MERRA2.ar_tag.Rutz.3hourly.20100101-20101231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20100101-20101231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'e8e28e69391514716b2c0d9150e8b490'
'MERRA2.ar_tag.Rutz.3hourly.20110101-20111231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20110101-20111231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '2770225ceafadc409b9a10b35a2ba9e1'
'MERRA2.ar_tag.Rutz.3hourly.20120101-20121231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20120101-20121231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'dcf7867080fb5b438c6ea724d29e5eb4'
'MERRA2.ar_tag.Rutz.3hourly.20130101-20131231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20130101-20131231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'd8fe91bd771f9fcf779b790b985f96d3'
'MERRA2.ar_tag.Rutz.3hourly.20140101-20141231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20140101-20141231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '2024d0df04f0d6caf8e6753ee36bb63b'
'MERRA2.ar_tag.Rutz.3hourly.20150101-20151231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20150101-20151231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '30593680b002af1bd3e843c77ff43bf9'
'MERRA2.ar_tag.Rutz.3hourly.20160101-20161231.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20160101-20161231.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '65795dacdfe3f4eda643fc3234a0e5fe'
'MERRA2.ar_tag.Rutz.3hourly.20170101-20170430.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/rutz/MERRA2.ar_tag.Rutz.3hourly.20170101-20170430.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '14b6fe99984006b6a804918ac9ee227f'
EOF--dataset.file.url.chksum_type.chksum

}


#
# MAIN
#
echo "Running $(basename $0) version: $version"

#do we have old results? Create the file if not
[ ! -f $CACHE_FILE ] && echo "#filename mtime checksum" > $CACHE_FILE

download

#remove duplicates (if any)
{ rm $CACHE_FILE && tac | awk '!x[$1]++' | tac > $CACHE_FILE; } < $CACHE_FILE
