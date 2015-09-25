mtype = { allocate_block, use_block };

chan c = [0] of { mtype };

active proctype block()
{
Free:
   printf("Block is Free\n");
   if
   :: c?allocate_block -> goto Allocated;
   fi

Allocated:
   printf("Block is Allocated\n");
   if
   :: c?use_block -> goto Used;
   :: c?allocate_block -> goto Illegal;
   fi

Used:
   printf("Block is Used\n");
   if
   :: c?use_block -> goto Illegal;
   :: c?allocate_block -> goto Illegal;
   fi

Illegal:
   printf("Block is Illegal\n");
}

active proctype master() {
J1:
   printf("Allocating block to slave\n");
   c!allocate_block;
   goto J2;

J2:
   printf("Slave using block\n");
   c!use_block;
   if
   :: printf("Master crash!\n");
      goto J1;
   :: goto Committed;
   fi

Committed:
   printf("Master is Committed\n");
}
