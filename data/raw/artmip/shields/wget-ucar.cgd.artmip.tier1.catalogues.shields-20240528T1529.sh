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
# Created: 2024-05-28T15:29:15-06:00
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
# ucar.cgd.artmip.tier1.catalogues.shields
# ed29ce27-400e-453e-8a49-e28580f5111d
# https://www.earthsystemgrid.org/dataset/ucar.cgd.artmip.tier1.catalogues.shields.html
# https://www.earthsystemgrid.org/dataset/id/ed29ce27-400e-453e-8a49-e28580f5111d.html
#
# Dataset Version
# 1
# 1d75e270-f4cd-4a47-832e-cf31baee97d6
# https://www.earthsystemgrid.org/dataset/ucar.cgd.artmip.tier1.catalogues.shields/version/1.html
# https://www.earthsystemgrid.org/dataset/version/id/1d75e270-f4cd-4a47-832e-cf31baee97d6.html
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
'MERRA2.ar_tag.Shields_v1.3hourly.2000.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2000.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'b7ed1ef2ed31d7232c58fa23e9dc7523'
'MERRA2.ar_tag.Shields_v1.3hourly.2001.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2001.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'd3d2832ad9164e4ca2aadc83ab568128'
'MERRA2.ar_tag.Shields_v1.3hourly.2002.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2002.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'ee19158d8614924c97970af531eeb35b'
'MERRA2.ar_tag.Shields_v1.3hourly.2003.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2003.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '025370f997b2314f44dca914f60b9eb5'
'MERRA2.ar_tag.Shields_v1.3hourly.2004.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2004.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '3128839ec072f2b6d70bdfca9a279aa6'
'MERRA2.ar_tag.Shields_v1.3hourly.2005.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2005.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '6c3e41150bf1574e059579099c1a2c32'
'MERRA2.ar_tag.Shields_v1.3hourly.2006.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2006.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '56181cca3d84a67d144b7db8e7c00c3a'
'MERRA2.ar_tag.Shields_v1.3hourly.2007.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2007.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '929651a34c9ecaf49de62265dcc3fefd'
'MERRA2.ar_tag.Shields_v1.3hourly.2008.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2008.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'daf7f48082b40831ff60fb3771ec854d'
'MERRA2.ar_tag.Shields_v1.3hourly.2009.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2009.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '785ce350356ebae1108793cd76d9ebc0'
'MERRA2.ar_tag.Shields_v1.3hourly.2010.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2010.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '0d1f113f82072f28f9ea57281709d462'
'MERRA2.ar_tag.Shields_v1.3hourly.2011.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2011.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '53e1105c2ef1ec859e87598bce3a136c'
'MERRA2.ar_tag.Shields_v1.3hourly.2012.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2012.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'ccbf0ef20af477bbd206e0021173d7dd'
'MERRA2.ar_tag.Shields_v1.3hourly.2013.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2013.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'bcfc5e3fc5b1911a1a9e8369efb10ec7'
'MERRA2.ar_tag.Shields_v1.3hourly.2014.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2014.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '2df5ac0aa53729f6c16ddd953080ff3c'
'MERRA2.ar_tag.Shields_v1.3hourly.2015.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2015.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'ce929f12cadc3dfa0b8b370b6f74e1fa'
'MERRA2.ar_tag.Shields_v1.3hourly.2016.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2016.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '24e28e57cb44760d23967cb4100f564a'
'MERRA2.ar_tag.Shields_v1.3hourly.2017.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/shields/MERRA2.ar_tag.Shields_v1.3hourly.2017.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '04f2cded87e3ba83bbcb1e6215932725'
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
