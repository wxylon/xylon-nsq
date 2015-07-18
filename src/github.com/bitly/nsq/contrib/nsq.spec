%define name nsq
%define version 0.2.19
%define release 1
%define path usr/local
%define group Database/Applications
%define __os_install_post %{nil}

Summary:    nsq
Name:       %{name}
Version:    %{version}
Release:    %{release}
Group:      %{group}
Packager:   Matt Reiferson <mattr@bit.ly>
License:    Apache
BuildRoot:  %{_tmppath}/%{name}-%{version}-%{release}
AutoReqProv: no
# we just assume you have go installed. You may or may not have an RPM to depend on.
# BuildRequires: go

%description 
nsq - realtime distributed message processing at scale
https://github.com/bitly/nsq

%prep
mkdir -p $RPM_BUILD_DIR/%{name}-%{version}-%{release}
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}
git clone git@github.com:bitly/nsq.git

%build
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/nsq
make PREFIX=/%{path}

%install
export DONT_STRIP=1
rm -rf $RPM_BUILD_ROOT
cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/nsq
# binaries and template data
make PREFIX=/${path} DESTDIR=$RPM_BUILD_ROOT install

%files
/%{path}/bin/nsqadmin
/%{path}/bin/nsqd
/%{path}/bin/nsqlookupd
/%{path}/bin/nsq_to_file
/%{path}/bin/nsq_pubsub
/%{path}/bin/nsq_to_http
/%{path}/share/nsqadmin/templates
