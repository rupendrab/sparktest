create table word_counts
(
 word   character varying(100),
 cnt    integer
);

alter table word_counts add constraint pk_word_counts primary key (word);

create or replace function update_word_counts
(
  words     varchar,
  counts    varchar
)
returns int
as
$$
declare
  ln_affected    int;
begin
  with ndata as
  (
    select word,
           cnt::int cnt
    from
    (
      select regexp_split_to_table(words, '\s+') word,
             regexp_split_to_table(counts, '\s+') cnt
    ) a
    where a.cnt != ''
  )
  insert into word_counts (word, cnt)
  select word, cnt from ndata
  on conflict(word)
  do update
  set cnt = EXCLUDED.cnt;

  get diagnostics ln_affected = ROW_COUNT;
  return ln_affected;
end
$$
language plpgsql volatile;

create table word_counts_by_file
(
 fname  character varying(100),
 word   character varying(100),
 cnt    integer
);

alter table word_counts_by_file add constraint pk_word_counts_by_file primary key (fname, word);

create or replace function update_word_counts_by_file
(
  fnames    varchar,
  words     varchar,
  counts    varchar
)
returns int
as
$$
declare
  ln_affected    int;
begin
  with ndata as
  (
    select fname,
           word,
           cnt::int cnt
    from
    (
      select regexp_split_to_table(fnames, '\s+') fname,
             regexp_split_to_table(words, '\s+') word,
             regexp_split_to_table(counts, '\s+') cnt
    ) a
    where a.cnt != ''
  )
  insert into word_counts_by_file (fname, word, cnt)
  select fname, word, cnt from ndata
  on conflict(fname, word)
  do update
  set cnt = EXCLUDED.cnt;

  get diagnostics ln_affected = ROW_COUNT;
  return ln_affected;
end
$$
language plpgsql volatile;

