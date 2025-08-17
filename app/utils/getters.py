from typing import Optional, Union

from app.domain.inst import Inst, PrimaryInst, FlatInst


def get_inst_id(inst: Union[Optional[Inst],Optional[FlatInst]]) -> Optional[str]:
    return inst.id if isinstance(inst, (Inst, FlatInst)) else None


def get_inst(inst: Optional[Inst]) -> Optional[Inst]:
    return Inst(id=inst.id) if isinstance(inst, Inst) else None


def get_primary_inst(inst: Optional[PrimaryInst]) -> Optional[PrimaryInst]:
    return PrimaryInst(id=inst.id, primary_id=inst.primary_id) if isinstance(inst, PrimaryInst) else None

