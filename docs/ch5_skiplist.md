# level
纯技术问题，也要写use case.why？

support iterate all nodes in this level.

## public properties

## private properties

## operations
### put(key, value)
- findNodeInPage(page, key);
```
while page is not nil:
	for node from page:
		if node.key > key:
			return node
		end if
	end for
	page = page.next
end while
return nil
```
- insert(key, value)
```
new_node = Node(key, value)
level = get level from metainfo
page = nil
do 
	page = get first page of level by metainfo
	node = findNodeFrom(page, key)
	level --
	if node is nil:
		page = get first page of level by metainfo
	else:
		page = node.next
	end if
while level > 0

if page is nil:
	page = allocate()
end if

outter:
if (key < page.lastKey or page.isEmpty) and (page.size + new_node.size  < page.capacity):
	insert key/value into the list of this page.
	mark page dirty
else
	if page.next is nil:
		nextPage = pageManager.createPage()
		next = page.next
		page.next = nextPage
		nextPage.next = next
		page = nextPage
	else:
		page = page.next
	end if
	goto outter:
end if
```

- promote(node)
```
i = node.index
if i is odd 
  if i is not the last node at level j
    randomly choose whether to promote it to level j+1
  else
    do not promote
  end if
else if i is even and node i-1 was not promoted
  promote it to level j+1
end if
```

create page:
```
pageNo = seq()
for heap in heaps:
    if heap.size > 4G:
		continue;
	end if
	page = get first free page of heap.
	if page is nil and heap.size + 4K <= 4G:
		page = heap.allocPage(pageNo);
	end if
end for
```
### del(key)
### get(key)
### range(begin, end)

# node
## public properties
level, key, value.

## private properties
page no.
right node page no.
right node key.
next node page no.
next node key.

## operations
### right()
right page no + right node key

### next()
next page no + next node key

## heap file
4g file, 65536个64k

x个page,每个page大小为s字节，heap文件为h字节。
x*8 + x*s = h
x = Math.floor(h / (8+s))

x*8 + x*64*1024 = 4*1024*1024*1024
x*65544 = 4294967296
x = 65528.00009764

heap:
	heads: head[x]
	pages: page[x]

head:
	page no: 4 bytes
	page location: 4 bytes

| page no (4 bytes) | page location (4 bytes) | ... | page | page |

## skip list in page,ascending order
level > 0:
| meta (1 byte) | right page number (4 bytes)| number of keys (4 bytes)| key len (4 bytes) | key | next level page no (4 bytes) | key len | key | next level page no | ...

level 0:
| meta (1 byte) | right page number (4 bytes)| number of keys (4 bytes)| key len (4 bytes) | key | value len (4 bytes) | value | ...
