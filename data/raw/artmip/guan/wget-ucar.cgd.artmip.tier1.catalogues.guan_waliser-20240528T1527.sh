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
# Created: 2024-05-28T15:27:33-06:00
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
# ucar.cgd.artmip.tier1.catalogues.guan_waliser
# 1b9c1546-438a-4901-8ddf-cb538f6af327
# https://www.earthsystemgrid.org/dataset/ucar.cgd.artmip.tier1.catalogues.guan_waliser.html
# https://www.earthsystemgrid.org/dataset/id/1b9c1546-438a-4901-8ddf-cb538f6af327.html
#
# Dataset Version
# 1
# 01d5c77d-cad4-4e91-a9aa-82685b3dcb1f
# https://www.earthsystemgrid.org/dataset/ucar.cgd.artmip.tier1.catalogues.guan_waliser/version/1.html
# https://www.earthsystemgrid.org/dataset/version/id/01d5c77d-cad4-4e91-a9aa-82685b3dcb1f.html
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
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2000.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2000.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '4b1a63f6e9571cd586876d348f03f755'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2001.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2001.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'af00993adae388d6ecbe1ac2b8b70011'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2002.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2002.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'b0fbf8a7f00a01f1b6d73a39c4f35ef6'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2003.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2003.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '900fc4d2535baa1bbc28a284b90d1d03'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2004.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2004.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '082340390fb6199661670e70159413f3'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2005.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2005.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '0ea21e7f979601dfe4f29ef1ba66d6b9'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2006.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2006.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'bf709b638731a7720418c1075a57674b'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2007.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2007.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '13e9455bbb43f468d538c1da52cbdf42'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2008.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2008.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '21b185c004317face39a57b955be287b'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2009.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2009.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '979ce1f20f950691f25aa80ee9a3ac13'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2010.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2010.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '7e10bafe0391fe3e97a95a6577400f6b'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2011.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2011.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'f0027720b0e4173252845a90cf5e3e33'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2012.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2012.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '4f96451c045609d924be2af869af95c1'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2013.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2013.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '518c252a04060098f5a35061e07caeca'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2014.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2014.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'c44ee91e7ea4aab133055bf5369347ac'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2015.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2015.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '6795605b99bab679d80d4c7a39b0d87d'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2016.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2016.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' 'cc257fb0d7b077e6cdc74d6e6f51112a'
'MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2017.nc' 'https://tds.ucar.edu/thredds/fileServer/datazone/cdg/data/ARTMIP/catalogues/tier1/guan_waliser/MERRA2.ar_tag.Guan_Waliser_v2.3hourly.2017.nc?api-token=I9NCyMhzSmdhQUudzkCFoIAxHWFSAunT4bYoZsK8' 'md5' '59eeb1539d1808d478b574e3b5dcb89e'
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
