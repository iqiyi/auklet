%define _version            %(git describe --tags | tr - .)
%define _topdir             %{getenv:BUILDDIR}
%global auklet_user    swift

Name:   auklet
Version:  %{_version}
Release:  1%{?dist}
Summary:  Auklet is an reimplementation of OpenStack Swift Object Server in Golang

License: Apache License v2
URL: https://github.com/iqiyi/auklet
Source0:  %{name}-%{version}.tar.gz
Source10: auklet-object.service

Requires(post):    systemd
Requires(preun):   systemd
Requires(postun):  systemd

%description
%{summary}


%prep
%setup -q

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/usr/local/bin
cp auklet %{buildroot}/usr/local/bin
install -p -D -m 0644 %{SOURCE10} \
    %{buildroot}%{_unitdir}/auklet-object.service

%pre
getent group %{auklet_user} > /dev/null || groupadd -r %{auklet_user}
getent passwd %{auklet_user} > /dev/null || \
    useradd -r -M -g %{auklet_user} -s /sbin/nologin -c "openstack swift user" %{auklet_user}
exit 0

%post
%systemd_post auklet-object.service

%preun
%systemd_preun auklet-object.service

%postun
%systemd_postun auklet-object.service

%files
/usr/local/bin/auklet
%{_unitdir}/auklet-object.service
