create table summation
(
    id_date_user      varchar(50) not null
        primary key,
    id_date           int         not null,
    id_user           int         not null,
    call_sum          int         not null comment '总通话次数',
    call_duration_sum int         not null comment '总通话时长'
);

create table date
(
    id    int auto_increment
        primary key,
    year  int not null,
    month int not null,
    day   int not null
);

create table user
(
    id    int auto_increment comment '用户id'
        primary key,
    phone varchar(20) collate utf8mb4_bin not null comment '电话号',
    name  varchar(50) collate utf8mb4_bin not null comment '用户名'
)
    charset = latin1;

