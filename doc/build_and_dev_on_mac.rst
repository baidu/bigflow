Disclaimer
############
Bigflow don't support running on Mac, this article is for developers only.

Building on Mac
#################

Dependency
"""""""""""""""""
1. XCode and its command line tools::

    xcode-select --install

2. JDK(>=1.8), please goto http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html
   after installation, set JAVA_HOME::

    export JAVA_HOME=`/usr/libexec/java_home -v 1.8`

   You can add this line to your shell init file(~/.bash_profile or ~/.zshrc, etc.)

3. Third party tools::

    brew install openssl bison libidn libgcrypt maven autoconf cmake libtool

Building
""""""""""""""""
Execute commands below should built all the targets defined in the CMakeLists.txt::

    # in case you don't persist this to .bash_profile or .zshrc
    export JAVA_HOME=`/usr/libexec/java_home -v 1.8`
    # use brew installed bison instead as thrift requires that
    export PATH=/usr/local/opt/bison/bin:$PATH
    mkdir build
    cd build
    cmake ../

    make -j 3

Developing
"""""""""""""""
We use CLion(https://www.jetbrains.com/clion/) as our primary dev tool.
Open CMakeLists.txt as a project should setup everything smoothly.